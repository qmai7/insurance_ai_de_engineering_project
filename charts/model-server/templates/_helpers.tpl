{{/*
The Service name, and therefore the host the API calls and the host the
VirtualService matches on. Fixed rather than release-prefixed: it is baked into
the API's MODEL_SERVER_PREDICT_URL default and into the mesh routing rules, so
a release rename must not silently move it.
*/}}
{{- define "model-server.name" -}}
model-server
{{- end -}}

{{/*
Labels shared by both roles. `version` is deliberately NOT here — it is what
distinguishes champion from challenger, and the DestinationRule's subsets
select on it.
*/}}
{{- define "model-server.labels" -}}
app.kubernetes.io/name: {{ include "model-server.name" . }}
app.kubernetes.io/part-of: fraud-serving
{{- end -}}

{{/*
The Service selector. Only the shared labels, so a single Service fronts both
roles' pods — which is the precondition for splitting traffic between them.
Adding `version` here would give each role its own endpoint set and there would
be nothing left for the mesh to decide.
*/}}
{{- define "model-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "model-server.name" . }}
{{- end -}}
