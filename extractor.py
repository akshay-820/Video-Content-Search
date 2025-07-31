import os
import asyncio
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import torch
from PIL import Image
import open_clip
import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
from gi.repository import Gst, GstApp, GLib

# --- Configuration ---
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://registry:8000")
EXTRACTOR_ID = os.getenv("EXTRACTOR_ID", "extractor-1")
EXTRACTOR_URL = os.getenv("EXTRACTOR_URL", "http://extractor-1:9000")
VECTOR_DB_URL = os.getenv("VECTOR_DB_URL", "http://milvus-or-qdrant:19530") # Placeholder for actual DB

# --- Models & Preprocessing ---
print("Loading OpenCLIP model...")
model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='laion2b_s34b_b79k')
tokenizer = open_clip.get_tokenizer('ViT-B-32')
print("Model loaded successfully.")

# --- Pydantic Models ---
class JobRequest(BaseModel):
    video_uri: str
    job_id: str

class ExtractorRegistration(BaseModel):
    extractor_id: str
    extractor_url: str

# --- FastAPI App ---
app = FastAPI(title="Extractor Service")

# --- GStreamer Frame Extraction Logic ---
def process_stream(video_uri: str, job_id: str):
    """
    Connects to a video stream (file or RTSP) using GStreamer,
    extracts frames, and processes them to generate embeddings.
    """
    print(f"[{job_id}] Starting GStreamer pipeline for URI: {video_uri}")

    # GStreamer pipeline description. Works for files and RTSP streams.
    # It decodes the stream and converts frames to raw RGB format for processing.
    pipeline_desc = (
        f"uridecodebin uri={video_uri} ! "
        "videoconvert ! "
        "video/x-raw,format=RGB ! "
        "appsink name=sink emit-signals=true"
    )

    pipeline = Gst.parse_launch(pipeline_desc)
    appsink = pipeline.get_by_name('sink')

    # This handler is called for each new frame
    def on_new_sample(sink):
        sample = sink.emit('pull-sample')
        if sample:
            buf = sample.get_buffer()
            caps = sample.get_caps()
            
            width = caps.get_structure(0).get_value('width')
            height = caps.get_structure(0).get_value('height')
            
            # Map buffer to a readable format
            result, mapinfo = buf.map(Gst.MapFlags.READ)
            if result:
                # Create a PIL Image from the raw RGB data
                image = Image.frombytes('RGB', (width, height), mapinfo.data)
                
                # --- CLIP Embedding Generation ---
                with torch.no_grad():
                    # Preprocess the image and generate the embedding
                    image_processed = preprocess(image).unsqueeze(0)
                    image_features = model.encode_image(image_processed)
                    image_features /= image_features.norm(dim=-1, keepdim=True)
                
                # TODO: Send to Vector Database
                # This is where you would send `image_features` to Milvus/Qdrant
                print(f"[{job_id}] Generated embedding for a frame. Shape: {image_features.shape}")

                buf.unmap(mapinfo)
        return Gst.FlowReturn.OK

    appsink.connect('new-sample', on_new_sample)

    # Start the pipeline
    pipeline.set_state(Gst.State.PLAYING)

    try:
        # We run a GLib MainLoop to keep the pipeline running.
        # This is a simple way to handle it. In a real app, you might manage this loop better.
        loop = GLib.MainLoop()
        # For a real service, you'd need a more robust way to stop this loop.
        # For example, after a certain period of inactivity or on a specific signal.
        loop.run() 
    except KeyboardInterrupt:
        print(f"[{job_id}] Stopping pipeline.")
    finally:
        pipeline.set_state(Gst.State.NULL)
        print(f"[{job_id}] Pipeline stopped and resources released.")


# --- API Endpoints ---
@app.on_event("startup")
async def register_with_registry():
    """
    On startup, register this extractor instance with the central registry.
    """
    print("Attempting to register with the central registry...")
    payload = ExtractorRegistration(extractor_id=EXTRACTOR_ID, extractor_url=EXTRACTOR_URL)
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{REGISTRY_URL}/register", json=payload.dict())
            response.raise_for_status()
            print(f"Successfully registered with registry: {response.json()}")
        except httpx.RequestError as e:
            print(f"Error: Could not connect to registry at {REGISTRY_URL}. Is it running? Details: {e}")
        except httpx.HTTPStatusError as e:
            print(f"Error: Failed to register with registry. Status: {e.response.status_code}, Body: {e.response.text}")


@app.post("/extract")
async def extract_features(request: JobRequest, background_tasks: BackgroundTasks):
    """
    This is the main endpoint that receives a job from the input-router.
    It starts the GStreamer processing in the background.
    """
    print(f"Received job {request.job_id} for video: {request.video_uri}")
    
    # Run the GStreamer pipeline in the background so the API can respond immediately
    background_tasks.add_task(process_stream, request.video_uri, request.job_id)
    
    return {"message": "Job accepted. Processing started in the background.", "job_id": request.job_id}

@app.get("/health")
def health_check():
    return {"status": "ok"}