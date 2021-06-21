{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}


{{/*
Create a short app name.
*/}}
{{- define "splice.name" -}}
splice
{{- end -}}

{{/*
Create the domain name part of services.
The HDFS config file should specify FQDN of services. Otherwise, Kerberos
login may fail.
*/}}
{{- define "svc-domain" -}}
{{- printf "%s.svc.cluster.local" .Release.Namespace -}}
{{- end -}}


{{/*
Create a fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "splice.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "splice.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}



{{- define "kafkaconsumer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "kafkaconsumer.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}


{{- define "kafkaconsumer.labels" -}}
chart: "{{ .Chart.Name }}-{{ .Chart.Version }}"
app: "{{ template "kafkaconsumer.name" . }}"
release: {{ .Release.Name | quote}}
heritage: "{{ .Release.Service }}"
{{- end -}}


