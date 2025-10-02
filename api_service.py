from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import uuid
from typing import Optional
from pydantic import BaseModel
import database
import storage
import broker

app = FastAPI(title="Image Processing API")

class ProcessingRequest(BaseModel):
    processing_type: str
    parameters: Optional[dict] = None

@app.post("/upload-and-process")
async def upload_and_process(
    file: UploadFile = File(...),
    processing_type: str = "grayscale",
    parameters: Optional[dict] = None,
    db: Session = Depends(database.get_db)
):
    # Generate unique task ID
    task_id = str(uuid.uuid4())
    
    # Read file content
    file_content = await file.read()
    
    # Generate storage keys
    original_key = storage.generate_image_key("original")
    processed_key = storage.generate_image_key("processed")
    
    # Upload original file
    if not storage.upload_file(file_content, original_key):
        raise HTTPException(status_code=500, detail="Failed to upload original image")
    
    # Create task in database
    task = database.Task(
        task_id=task_id,
        original_image_key=original_key,
        processed_image_key=processed_key,
        processing_type=processing_type,
        status=database.TaskStatus.PENDING
    )
    db.add(task)
    db.commit()
    
    # Publish task to message queue
    task_data = {
        "task_id": task_id,
        "original_image_key": original_key,
        "processed_image_key": processed_key,
        "processing_type": processing_type,
        "parameters": parameters or {}
    }
    
    if not broker.publish_task(task_data):
        # If message publishing fails, update task status
        task.status = database.TaskStatus.FAILED
        task.error_message = "Failed to publish task to queue"
        db.commit()
        raise HTTPException(status_code=500, detail="Failed to queue processing task")
    
    return JSONResponse(
        status_code=202,
        content={
            "task_id": task_id,
            "status": "PENDING",
            "message": "Task accepted for processing"
        }
    )

@app.get("/status/{task_id}")
async def get_task_status(task_id: str, db: Session = Depends(database.get_db)):
    task = db.query(database.Task).filter(database.Task.task_id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    response = {
        "task_id": task.task_id,
        "status": task.status.value,
        "created_at": task.created_at.isoformat(),
        "updated_at": task.updated_at.isoformat()
    }
    
    if task.status == database.TaskStatus.COMPLETED:
        response["processed_image_url"] = storage.get_file_url(task.processed_image_key)
    elif task.status == database.TaskStatus.FAILED:
        response["error"] = task.error_message
    
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
