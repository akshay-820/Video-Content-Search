import os
import gi
import uvicorn
import httpx
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

gi.require_version('Gst', '1.0')
from gi.repository import Gst

Gst.init(None)

# --- Configuration from Environment Variables ---
EXTRACTOR_ID = os.getenv("EXTRACTOR_ID", "default_extractor")
EXTRACTOR_URL = os.getenv("EXTRACTOR_URL", "http://localhost:8001")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://registry:8000")
# Frames are staged in this directory *inside the container*
OUTPUT_DIR_BASE = "/app/extracted_frames"

class GStreamerSceneCutExtractor:
    def extract_scene_changes(self, input_uri, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # MODIFIED PIPELINE: Using filesrc and decodebin for better reliability
        pipeline_desc = f"""
            filesrc location="{input_uri}" ! decodebin name=decode
            decode. ! videoconvert ! videorate ! video/x-raw,framerate=1/5 !
            jpegenc !
            multifilesink location="{output_dir}/frame-%05d.jpg"
        """
        pipeline = Gst.parse_launch(pipeline_desc)
        bus = pipeline.get_bus()

        print(f"[{EXTRACTOR_ID}] Starting pipeline for {input_uri}...")
        pipeline.set_state(Gst.State.PLAYING)
        try:
            msg = bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.EOS | Gst.MessageType.ERROR)
            if msg and msg.type == Gst.MessageType.ERROR:
                err, debug = msg.parse_error()
                print(f"[{EXTRACTOR_ID}] GStreamer Error: {err} {debug}")
        finally:
            pipeline.set_state(Gst.State.NULL)
            print(f"[{EXTRACTOR_ID}] Pipeline finished. Frames staged in {output_dir}")

# --- Extractor Service Logic ---
def run_extraction_job(video_uri: str):
    # This function uses a synchronous httpx client because it runs in a background thread
    with httpx.Client() as client:
        try:
            client.post(f"{REGISTRY_URL}/update_status?extractor_id={EXTRACTOR_ID}&status=busy")
        except httpx.RequestError as e:
            print(f"[{EXTRACTOR_ID}] Could not update status to busy: {e}")
            return # Exit if we can't update status

    output_directory = os.path.join(OUTPUT_DIR_BASE, os.path.basename(video_uri).replace('.', '_'))
    extractor = GStreamerSceneCutExtractor()
    extractor.extract_scene_changes(video_uri, output_directory)
    
    with httpx.Client() as client:
        try:
            client.post(f"{REGISTRY_URL}/update_status?extractor_id={EXTRACTOR_ID}&status=available")
        except httpx.RequestError as e:
            print(f"[{EXTRACTOR_ID}] Could not update status to available: {e}")

class JobRequest(BaseModel):
    video_uri: str

app = FastAPI(title="Extractor Service")

@app.on_event("startup")
def on_startup():
    # This function uses a synchronous httpx client because it runs once at startup
    with httpx.Client() as client:
        try:
            client.post(f"{REGISTRY_URL}/register", json={"extractor_id": EXTRACTOR_ID, "extractor_url": EXTRACTOR_URL})
            print(f"[{EXTRACTOR_ID}] Successfully registered with URL: {EXTRACTOR_URL}")
        except httpx.RequestError as e:
            print(f"[{EXTRACTOR_ID}] Could not register with registry: {e}")

@app.post("/extract")
def extract(request: JobRequest, background_tasks: BackgroundTasks):
    print(f"[{EXTRACTOR_ID}] Received job for URI: {request.video_uri}")
    background_tasks.add_task(run_extraction_job, request.video_uri)
    return {"message": "Extraction job started in the background."}
