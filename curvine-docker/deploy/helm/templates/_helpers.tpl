{{/*
Expand the name of the chart.
*/}}
{{- define "curvine.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "curvine.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "curvine.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "curvine.labels" -}}
helm.sh/chart: {{ include "curvine.chart" . }}
{{ include "curvine.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "curvine.selectorLabels" -}}
app.kubernetes.io/name: {{ include "curvine.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Master specific labels
*/}}
{{- define "curvine.masterLabels" -}}
{{ include "curvine.labels" . }}
app.kubernetes.io/component: master
{{- end }}

{{/*
Master selector labels
*/}}
{{- define "curvine.masterSelectorLabels" -}}
{{ include "curvine.selectorLabels" . }}
app.kubernetes.io/component: master
{{- end }}

{{/*
Worker specific labels
*/}}
{{- define "curvine.workerLabels" -}}
{{ include "curvine.labels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Worker selector labels
*/}}
{{- define "curvine.workerSelectorLabels" -}}
{{ include "curvine.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Master fullname
*/}}
{{- define "curvine.masterFullname" -}}
{{- printf "%s-master" (include "curvine.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Worker fullname
*/}}
{{- define "curvine.workerFullname" -}}
{{- printf "%s-worker" (include "curvine.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Master service name (headless)
*/}}
{{- define "curvine.masterServiceName" -}}
{{- printf "%s-master" (include "curvine.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Worker service name (headless)
*/}}
{{- define "curvine.workerServiceName" -}}
{{- printf "%s-worker" (include "curvine.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "curvine.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "curvine.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Validate master replicas is odd number
*/}}
{{- define "curvine.validateMasterReplicas" -}}
{{- if not (mod (int .Values.master.replicas) 2) }}
{{- fail "master.replicas must be an odd number (1, 3, 5, 7...) for Raft consensus" }}
{{- end }}
{{- end }}

{{/*
Generate journal addresses dynamically
Format: {id = N, hostname = "...", port = 8996}
*/}}
{{- define "curvine.journalAddrs" -}}
{{- range $i := until (int .Values.master.replicas) -}}
    {id = {{ add $i 1 }}, hostname = "{{ include "curvine.masterFullname" $ }}-{{ $i }}.{{ include "curvine.masterServiceName" $ }}.{{ $.Release.Namespace }}.svc.{{ $.Values.global.clusterDomain }}", port = {{ $.Values.master.journalPort }}},
{{ end -}}
{{- end }}

{{/*
Generate master addresses dynamically
Format: { hostname = "...", port = 8995 }
*/}}
{{- define "curvine.masterAddrs" -}}
{{- range $i := until (int .Values.master.replicas) -}}
    { hostname = "{{ include "curvine.masterFullname" $ }}-{{ $i }}.{{ include "curvine.masterServiceName" $ }}.{{ $.Release.Namespace }}.svc.{{ $.Values.global.clusterDomain }}", port = {{ $.Values.master.rpcPort }} },
{{ end -}}
{{- end }}

{{/*
Generate worker data directories configuration
Format: ["[TYPE:SIZE]/path", ...]
*/}}
{{- define "curvine.workerDataDirs" -}}
{{- $dirs := list }}
{{- range .Values.worker.storage.dataDirs }}
{{- if .enabled }}
{{- $size := .size | default "100GB" }}
{{- /* Convert Kubernetes size format (Gi/Mi/Ki) to Curvine format (GB/MB/KB) */ -}}
{{- $size = $size | replace "Gi" "GB" | replace "Mi" "MB" | replace "Ki" "KB" | replace "Ti" "TB" | replace "Pi" "PB" }}
{{- $type := .type | default "SSD" | upper }}
{{- $path := include "curvine.absPath" (dict "path" .mountPath) }}
{{- $dirs = append $dirs (printf "[%s:%s]%s" $type $size $path) }}
{{- end }}
{{- end }}
{{- if not $dirs }}
{{- $dirs = append $dirs "[SSD:100GB]/data/data1" }}
{{- end }}
[{{- range $index, $dir := $dirs }}{{- if $index }}, {{ end }}"{{ $dir }}"{{ end -}}]
{{- end }}

{{/*
Image pull secrets
*/}}
{{- define "curvine.imagePullSecrets" -}}
{{- if .Values.image.pullSecrets }}
imagePullSecrets:
{{- range .Values.image.pullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Full image name
*/}}
{{- define "curvine.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag }}
{{- end }}

{{/*
Curvine home directory inside container
*/}}
{{- define "curvine.home" -}}
/app/curvine
{{- end }}

{{/*
Curvine config file path inside container
*/}}
{{- define "curvine.configFile" -}}
/app/curvine/conf/curvine-cluster.toml
{{- end }}

{{/*
Application home directory
*/}}
{{- define "curvine.appHome" -}}
/app
{{- end }}

{{/*
Ensure config paths are absolute inside the container
*/}}
{{- define "curvine.absPath" -}}
{{- $path := .path | default "" -}}
{{- $home := include "curvine.home" . -}}
{{- if or (eq $path "") (hasPrefix "/" $path) -}}
{{- $path -}}
{{- else -}}
{{- printf "%s/%s" $home $path -}}
{{- end -}}
{{- end }}

{{/*
Return the proper Storage Class for Master Meta
*/}}
{{- define "curvine.masterMetaStorageClass" -}}
{{- if .Values.master.storage.meta.storageClass -}}
{{- .Values.master.storage.meta.storageClass -}}
{{- end -}}
{{- end }}

{{/*
Return the proper Storage Class for Master Journal
*/}}
{{- define "curvine.masterJournalStorageClass" -}}
{{- if .Values.master.storage.journal.storageClass -}}
{{- .Values.master.storage.journal.storageClass -}}
{{- end -}}
{{- end }}

{{/*
Worker anti-affinity rules
*/}}
{{- define "curvine.workerAntiAffinity" -}}
{{- if .Values.worker.antiAffinity.enabled }}
{{- if eq .Values.worker.antiAffinity.type "required" }}
podAntiAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchLabels:
          {{- include "curvine.workerSelectorLabels" . | nindent 10 }}
      topologyKey: kubernetes.io/hostname
{{- else }}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            {{- include "curvine.workerSelectorLabels" . | nindent 12 }}
        topologyKey: kubernetes.io/hostname
{{- end }}
{{- end }}
{{- end }}

{{/*
Validate master replicas - ensure they cannot be changed on upgrade
*/}}
{{- define "curvine.validateMasterReplicasOnUpgrade" -}}
{{- if .Release.IsUpgrade }}
{{- $currentReplicas := (lookup "apps/v1" "StatefulSet" .Release.Namespace (include "curvine.masterFullname" .)).spec.replicas | default 0 }}
{{- if and (ne $currentReplicas 0) (ne $currentReplicas (int .Values.master.replicas)) }}
{{- fail (printf "ERROR: Master replicas cannot be changed during upgrade! Current: %d, Requested: %d. To change master replicas, delete and redeploy the cluster." $currentReplicas (int .Values.master.replicas)) }}
{{- end }}
{{- end }}
{{- end }}
