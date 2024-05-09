# Use Python 3.11 base image
FROM python:3.11

# Set working directory in the container
WORKDIR /app

# Copy requirements.txt from host to container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Command to run the application
CMD ["python", "kafka_consumer.py"]
