import uvicorn
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

REGISTRY_URL = "http://registry:8000"

class VideoSourceRequest(BaseModel):
    video_uri: str

app = FastAPI(title="Input Source Router")

@app.post("/process_video")
async def process_video(request: VideoSourceRequest):
    async with httpx.AsyncClient() as client:
        try:
            # Step 1: Ask registry for an available extractor's URL
            response = await client.get(f"{REGISTRY_URL}/get_available_extractor")
            response.raise_for_status()
            extractor_info = response.json()
            extractor_url = f"{extractor_info['extractor_url']}/extract"
            
            # Step 2: Dispatch job to the (currently non-existent) extractor
            job_payload = {"video_uri": request.video_uri}
            dispatch_response = await client.post(extractor_url, json=job_payload)
            dispatch_response.raise_for_status()
            
            return {"message": "Job dispatched successfully", "dispatched_to": extractor_info}
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 503:
                raise HTTPException(status_code=503, detail="All extractors are busy or unavailable.")
            else:
                raise HTTPException(status_code=e.response.status_code, detail=f"Error from downstream service: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"Cannot connect to downstream service: {str(e)}")