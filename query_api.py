import os
import torch
import open_clip
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymilvus import Collection, connections

# --- CONFIGURATION ---
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus-standalone")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
COLLECTION_NAME = "video_frames"
TOP_K = 5 # Number of results to return

# --- MODEL LOADING ---
print("Query API: Loading OpenCLIP model...")
model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='laion2b_s34b_b79k')
device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)
tokenizer = open_clip.get_tokenizer('ViT-B-32')
print(f"Query API: Model loaded on device: {device}")

# --- FASTAPI APP & MILVUS CONNECTION ---
app = FastAPI(title="Query Processor API")
collection = None

@app.on_event("startup")
def startup_event():
    global collection
    try:
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
        collection = Collection(COLLECTION_NAME)
        collection.load() # Load collection into memory for searching
        print("Query API: Milvus connection successful and collection loaded.")
    except Exception as e:
        print(f"Query API: Error connecting to Milvus or loading collection: {e}")
        # In a real app, you might want to exit or have a retry mechanism
        
class TextSearchRequest(BaseModel):
    query: str

@app.post("/search/text")
async def search_by_text(request: TextSearchRequest):
    if not collection:
        raise HTTPException(status_code=503, detail="Milvus collection not loaded.")
        
    try:
        # [cite_start]1. Vectorize the text query via CLIP [cite: 1]
        text = tokenizer([request.query]).to(device)
        with torch.no_grad(), torch.cuda.amp.autocast():
            text_features = model.encode_text(text)
            text_features /= text_features.norm(dim=-1, keepdim=True)
        
        search_vector = text_features.cpu().numpy()[0].tolist()
        
        # [cite_start]2. Query Milvus for nearest matches [cite: 1]
        search_params = {
            "metric_type": "L2",
            "params": {"nprobe": 10},
        }
        
        results = collection.search(
            data=[search_vector],
            anns_field="embedding",
            param=search_params,
            limit=TOP_K,
            output_fields=["video_id", "frame_path"] # Specify which metadata to return
        )
        
        # 3. Format and return results
        response = []
        for hit in results[0]:
            response.append({
                "id": hit.id,
                "distance": hit.distance,
                "video_id": hit.entity.get("video_id"),
                "frame_path": hit.entity.get("frame_path")
            })
            
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during search: {str(e)}")