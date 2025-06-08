# Use an official lightweight Python image as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy and install all requirements in one go.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy our application code into the container
COPY . .

# Specify the command to run on container startup
CMD ["python", "main.py"]