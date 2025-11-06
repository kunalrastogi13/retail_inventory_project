#  Base image
FROM python:3.10-slim

#  Set working directory inside container
WORKDIR /app

#  Copy all project files into container
COPY . /app

# 📦 Install required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Keep the container running
CMD ["tail", "-f", "/dev/null"]