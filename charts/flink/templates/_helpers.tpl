{{/*
Flink configuration shared by both roles. The official image's entrypoint
appends FLINK_PROPERTIES to conf/flink-conf.yaml at startup, so this is the
whole config surface — no ConfigMap to keep in sync.

The fixed blob and TaskManager RPC ports matter on Kubernetes: their defaults
are random, which a Service cannot route to.
*/}}
{{- define "flink.properties" -}}
jobmanager.rpc.address: flink-jobmanager
jobmanager.rpc.port: 6123
blob.server.port: 6124
taskmanager.rpc.port: 6122
rest.port: 8081
rest.address: flink-jobmanager
jobmanager.bind-host: 0.0.0.0
taskmanager.bind-host: 0.0.0.0
rest.bind-address: 0.0.0.0
taskmanager.numberOfTaskSlots: {{ .Values.taskSlots }}
jobmanager.memory.process.size: {{ .Values.jobManagerProcessMemory }}
taskmanager.memory.process.size: {{ .Values.taskManagerProcessMemory }}
{{- end -}}
