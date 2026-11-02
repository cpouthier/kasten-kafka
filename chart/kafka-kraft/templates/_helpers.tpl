{{- define "kafka-kraft.selectorLabels" -}}
app.kubernetes.io/name: kafka-kraft
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "kafka-kraft.labels" -}}
{{ include "kafka-kraft.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{- define "kafka-kraft.headless" -}}
{{ printf "%s-headless" .Release.Name }}
{{- end }}
