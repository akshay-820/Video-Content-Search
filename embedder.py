import os
import time
import torch
import open_clip
from PIL import Image
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)

# --- CONFIGURATION ---
# Milvus connection details
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus-standalone")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")

# Milvus collection details
COLLECTION_NAME = "video_frames"
EMBEDDING_DIM = 512 # Based on the 'ViT-B-32' CLIP model

# Shared directories
FRAME_DIR = "/app/extracted_frames"
PROCESSED_MARKER = ".processed"

# Batching configuration
BATCH_SIZE = 100

# --- MODEL LOADING ---
print("Loading OpenCLIP model...")
model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='laion2b_s34b_b79k')
device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)
print(f"Model loaded on device: {device}")

# --- MILVUS HELPER FUNCTIONS ---
def get_milvus_collection():
    """Connects to Milvus and returns the collection object, creating it if it doesn't exist."""
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)

    if utility.has_collection(COLLECTION_NAME):
        print(f"Milvus collection '{COLLECTION_NAME}' already exists.")
        return Collection(COLLECTION_NAME)

    print(f"Milvus collection '{COLLECTION_NAME}' not found. Creating...")
    fields = [
        FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="video_id", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="frame_path", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM)
    ]
    schema = CollectionSchema(fields, "Video frame embeddings")
    collection = Collection(COLLECTION_NAME, schema)

    # Create an index for the embedding field for efficient searching
    index_params = {
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128}
    }
    collection.create_index("embedding", index_params)
    print("Milvus collection created and index built.")
    return collection

# --- EMBEDDER PULL WORKER LOGIC ---
def process_frames():
    collection = get_milvus_collection()
    
    while True:
        try:
            # List all video frame folders in the shared directory
            video_folders = [d for d in os.listdir(FRAME_DIR) if os.path.isdir(os.path.join(FRAME_DIR, d))]
            
            found_new_work = False
            for folder_name in video_folders:
                folder_path = os.path.join(FRAME_DIR, folder_name)
                marker_path = os.path.join(folder_path, PROCESSED_MARKER)

                # Pull Mode: Check if folder has been processed. If not, process it.
                if not os.path.exists(marker_path):
                    found_new_work = True
                    print(f"Found new folder to process: {folder_name}")
                    
                    frame_files = sorted([f for f in os.listdir(folder_path) if f.endswith(('.jpg', '.png'))])
                    
                    # Vector Cache / Batcher logic
                    batch_data = []
                    for frame_file in frame_files:
                        try:
                            image_path = os.path.join(folder_path, frame_file)
                            image = preprocess(Image.open(image_path)).unsqueeze(0).to(device)
                            
                            with torch.no_grad(), torch.cuda.amp.autocast():
                                image_features = model.encode_image(image)
                                image_features /= image_features.norm(dim=-1, keepdim=True)
                            
                            # Add to in-memory batch
                            batch_data.append({
                                "video_id": folder_name,
                                "frame_path": image_path,
                                "embedding": image_features.cpu().numpy().flatten().tolist()
                            })
                            
                            # Insert batch when full
                            if len(batch_data) >= BATCH_SIZE:
                                print(f"Inserting batch of {len(batch_data)} vectors into Milvus...")
                                collection.insert([
                                    [d["video_id"] for d in batch_data],
                                    [d["frame_path"] for d in batch_data],
                                    [d["embedding"] for d in batch_data],
                                ])
                                collection.flush()
                                batch_data = [] # Clear the batch

                        except Exception as e:
                            print(f"Error processing frame {frame_file}: {e}")

                    # Insert any remaining vectors in the last batch
                    if batch_data:
                        print(f"Inserting final batch of {len(batch_data)} vectors into Milvus...")
                        collection.insert([
                            [d["video_id"] for d in batch_data],
                            [d["frame_path"] for d in batch_data],
                            [d["embedding"] for d in batch_data],
                        ])
                        collection.flush()
                    
                    # Mark the folder as processed to avoid re-doing work
                    with open(marker_path, 'w') as f:
                        f.write('processed')
                    print(f"Finished processing and marked folder as done: {folder_name}")

            if not found_new_work:
                # If no work was found, wait before scanning again
                time.sleep(10)
        
        except Exception as e:
            print(f"An unexpected error occurred in the main loop: {e}")
            time.sleep(30) # Wait longer after a major error

if __name__ == "__main__":
    process_frames()