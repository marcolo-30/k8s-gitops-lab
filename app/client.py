# dev/client-deployment.yaml - Client Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-client
  namespace: myapp
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api-client
  template:
    metadata:
      labels:
        app: api-client
    spec:
      nodeSelector:
        kubernetes.io/hostname: r3-node
      containers:
      - name: client-container
        image: ghcr.io/marcolo-30/k8s-gitops-lab:f7750bbcf722957d89becd199434536dde0a8204
        imagePullPolicy: Always
        command: ["/bin/sh", "-c"]
        args:
        - |
          pip install Pillow requests opentelemetry-sdk opentelemetry-api \
                      opentelemetry-exporter-otlp-proto-http --quiet && \
          python client.py --continuous
        env:
        - name: SERVICE_ENDPOINT
          value: "http://image-processor-naive-svc:8080"
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: "http://192.168.0.42:4318"
        - name: SERVICE_NAME
          value: "image-processor-client"
        - name: MAX_RETRY_DELAY
          value: "30.0"
        - name: REQUEST_TIMEOUT
          value: "60.0"