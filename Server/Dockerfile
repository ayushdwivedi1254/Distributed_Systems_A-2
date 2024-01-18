# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org flask

# Make port 5000 available to the world outside this container
EXPOSE 5001

# Define environment variable
ENV SERVER_ID Unknown

# Run app.py when the container launches
CMD ["python", "server.py"]
