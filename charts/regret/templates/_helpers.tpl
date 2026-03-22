{{- define "regret.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "regret.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "regret.labels" -}}
app.kubernetes.io/name: {{ include "regret.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "regret.selectorLabels" -}}
app.kubernetes.io/name: {{ include "regret.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "regret.adapterNamespace" -}}
{{- default .Release.Namespace .Values.adapterNamespace }}
{{- end }}

{{- define "regret.pilotGrpcAddr" -}}
{{- if .Values.pilotGrpcAddr }}
{{- .Values.pilotGrpcAddr }}
{{- else }}
{{- printf "%s.%s.svc.cluster.local:%d" (include "regret.fullname" .) .Release.Namespace (.Values.service.grpcPort | int) }}
{{- end }}
{{- end }}
