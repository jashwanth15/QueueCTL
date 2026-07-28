# Use the official lightweight Python image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Run the workers by default when the container starts
CMD ["python", "queuectl.py", "worker", "start", "--count", "2"]
