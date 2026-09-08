{{- define "fraud-prediction-api.fullname" -}}
{{- printf "%s-%s" .Release.Name "fraud-prediction-api" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
