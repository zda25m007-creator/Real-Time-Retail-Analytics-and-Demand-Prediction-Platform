FROM python:3.11-slim
WORKDIR /app
COPY model_bundle.joblib .
COPY retail_api.py .
RUN pip install flask scikit-learn pandas numpy joblib
EXPOSE 3001
CMD ["python", "retail_api.py"]
