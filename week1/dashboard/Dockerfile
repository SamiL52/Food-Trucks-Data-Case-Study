FROM python:3.13-alpine

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY streamlit_dashboard.py .
ENV AWS_DEFAULT_REGION=eu-west-2
ENV STREAMLIT_SERVER_FILEWATCHERTYPE=none

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_dashboard.py", "--server.port=8501", "server.address=0.0.0.0"]