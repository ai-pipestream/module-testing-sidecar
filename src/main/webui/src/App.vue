<template>
  <main class="container">
    <header class="topbar">
      <h1>Module Testing Sidecar</h1>
      <p>Run module-specific tests from a registered module list. Parser modules can use file uploads; all modules can use repository documents.</p>
    </header>

    <section class="card">
      <h2>1) Select a module</h2>
      <div class="row">
        <label for="module-select">Target module</label>
        <select id="module-select" v-model="selectedModuleName" @change="onModuleChanged" :disabled="loadingTargets || running">
          <option value="" disabled>Select module</option>
          <option
            v-for="target in targets"
            :key="target.serviceId || target.moduleName"
            :value="target.moduleName"
          >
            {{ target.displayName || target.moduleName }} ({{ target.parser ? 'parser' : 'non-parser' }})
          </option>
        </select>
      </div>

      <div v-if="selectedTarget" class="meta">
        <span>Version: {{ selectedTarget.version || 'n/a' }}</span>
        <span>Module: {{ selectedTarget.moduleName }}</span>
        <span>Healthy: {{ selectedTarget.healthy ? 'yes' : 'no' }}</span>
      </div>
      <div v-if="selectedTarget && selectedTarget.registrationError" class="meta warning">
        {{ selectedTarget.registrationError }}
      </div>
    </section>

    <section class="card" v-if="selectedTarget">
      <h2>2) Choose input</h2>

      <div v-if="selectedTarget.parser" class="mode-switch">
        <label>
          <input type="radio" value="upload" v-model="inputMode" />
          Upload file (parser mode)
        </label>
        <label>
          <input type="radio" value="repository" v-model="inputMode" />
          Repository document
        </label>
      </div>
      <p v-else class="mode-note">
        Non-parser modules use repository documents only.
      </p>

      <div v-if="showUploadInput" class="row">
        <label for="input-file">Input file</label>
        <input
          id="input-file"
          type="file"
          @change="onFileSelected"
        />
        <p v-if="selectedFile">Selected: {{ selectedFile.name }}</p>
      </div>

      <div class="row">
        <label for="repository-doc-select">Repository document</label>
        <select
          id="repository-doc-select"
          v-model="selectedRepositoryNodeId"
          @change="onRepositoryDocumentChanged"
          :disabled="showUploadInput && Boolean(selectedFile)"
        >
          <option value="" disabled>Choose document</option>
          <option
            v-for="doc in sortedRepositoryDocuments"
            :key="doc.nodeId"
            :value="doc.nodeId"
          >
            {{ doc.label }}
          </option>
        </select>
        <button type="button" @click="refreshRepositoryDocuments">Refresh documents</button>
      </div>
    </section>

    <section class="card">
      <h2>3) Module config</h2>
      <p class="muted">The target provides a JSON schema; you can edit the JSON below. A malformed JSON payload is rejected before calling the API.</p>
      <textarea
        v-model="moduleConfigText"
        placeholder='{"key":"value"}'
        rows="10"
      ></textarea>
      <div v-if="schemaError" class="warning">Schema load error: {{ schemaError }}</div>
    </section>

    <section class="card">
      <h2>4) Run</h2>
      <div class="row">
        <button
          type="button"
          @click="runTest"
          :disabled="running || !canRun"
        >
          {{ running ? 'Running...' : 'Run test' }}
        </button>
        <button type="button" @click="clearRunResult" :disabled="!runResult && !runError">Clear result</button>
        <button type="button" @click="fetchLastError" class="btn-secondary">View last server error</button>
      </div>
      <p v-if="runError" class="warning">{{ runError }}</p>
    </section>

    <section class="card result-card" v-if="runResult">
      <div class="result-header">
        <h2>Result</h2>
        <div class="result-actions">
          <label class="depth-control">
            Expand depth
            <input type="number" v-model.number="jsonDepth" min="1" max="20" />
          </label>
          <button type="button" class="btn-small" @click="jsonDepth = 1">Collapse</button>
          <button type="button" class="btn-small" @click="jsonDepth = 20">Expand all</button>
          <button type="button" class="btn-small btn-secondary" @click="copyResult">{{ copyLabel }}</button>
        </div>
      </div>
      <div v-if="resultSummary" class="result-summary">
        <span v-for="(val, key) in resultSummary" :key="key" class="summary-chip">
          <strong>{{ key }}:</strong> {{ val }}
        </span>
      </div>
      <div class="json-viewer-wrap">
        <VueJsonPretty
          :data="runResult"
          :deep="jsonDepth"
          :showLength="true"
          :showLine="false"
          :showDoubleQuotes="true"
          :showIcon="true"
          :collapsedOnClickBrackets="true"
        />
      </div>
    </section>
  </main>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import VueJsonPretty from 'vue-json-pretty'
import 'vue-json-pretty/lib/styles.css'

const API_BASE = (() => {
  const path = window.location.pathname || '/'
  const adminIndex = path.indexOf('/admin')
  if (adminIndex < 0) {
    return '/test-sidecar/v1'
  }
  const root = path.substring(0, adminIndex)
  return `${root}/test-sidecar/v1`.replace(/\/+/g, '/')
})()

const targets = ref([])
const selectedModuleName = ref('')
const selectedTarget = ref(null)
const loadingTargets = ref(false)
const loadingDocuments = ref(false)
const repositoryDocuments = ref([])
const sortedRepositoryDocuments = computed(() =>
  [...repositoryDocuments.value].sort((a, b) => (a.label || '').localeCompare(b.label || ''))
)
const selectedRepositoryNodeId = ref('')
const inputMode = ref('upload')
const selectedFile = ref(null)
const moduleConfigText = ref('{}')
const running = ref(false)
const schemaError = ref('')
const runError = ref('')
const runResult = ref(null)
const error = ref('')
const statusMessage = ref('')
const jsonDepth = ref(3)
const copyLabel = ref('Copy JSON')

const showUploadInput = computed(() =>
  Boolean(selectedTarget.value?.parser && inputMode.value === 'upload')
)

const canRun = computed(() => {
  if (!selectedTarget.value || running.value) {
    return false
  }

  if (selectedTarget.value.parser) {
    if (inputMode.value === 'upload') {
      return Boolean(selectedFile.value)
    }
  }

  return Boolean(selectedRepositoryNodeId.value)
})

const effectiveMode = computed(() => {
  if (selectedTarget.value?.parser) {
    return inputMode.value
  }
  return 'repository'
})

const resultSummary = computed(() => {
  const r = runResult.value
  if (!r || typeof r !== 'object') return null
  const summary = {}
  if (r.success !== undefined) summary['Status'] = r.success ? 'Success' : 'Failed'
  if (r.processingTimeMs ?? r.processing_time_ms)
    summary['Time'] = `${r.processingTimeMs ?? r.processing_time_ms}ms`
  if (r.moduleName ?? r.module_name)
    summary['Module'] = r.moduleName ?? r.module_name
  const outDoc = r.outputDoc ?? r.output_doc
  if (outDoc) {
    const title = outDoc.title || outDoc.docId || outDoc.doc_id || ''
    if (title) summary['Doc'] = title
    const metaCount = Object.keys(outDoc.metadata ?? outDoc.structured_data ?? {}).length
    if (metaCount > 0) summary['Metadata fields'] = metaCount
  }
  if (r.error) summary['Error'] = r.error
  return Object.keys(summary).length > 0 ? summary : null
})

const copyResult = async () => {
  try {
    const text = JSON.stringify(runResult.value, null, 2)
    await navigator.clipboard.writeText(text)
    copyLabel.value = 'Copied!'
    setTimeout(() => { copyLabel.value = 'Copy JSON' }, 2000)
  } catch (_e) {
    copyLabel.value = 'Copy failed'
    setTimeout(() => { copyLabel.value = 'Copy JSON' }, 2000)
  }
}

const deriveDefaultConfigFromSchema = (schema, current) => {
  let result = {}
  if (current && Object.keys(current).length > 0) {
    result = { ...current }
  }
  if (!schema || typeof schema !== 'object' || schema.type !== 'object' || !schema.properties) {
    return result
  }

  Object.entries(schema.properties).forEach(([key, value]) => {
    if (result[key] !== undefined) {
      return
    }
    if (value && typeof value === 'object' && Object.prototype.hasOwnProperty.call(value, 'default')) {
      result[key] = value.default
    }
  })

  return result
}

const loadTargets = async () => {
  loadingTargets.value = true
  error.value = ''
  try {
    const response = await fetch(`${API_BASE}/targets`)
    if (!response.ok) {
      throw new Error(`Failed loading modules: HTTP ${response.status}`)
    }
    const json = await response.json()
    const list = Array.isArray(json) ? json : (json.targets || [])
    targets.value = list.filter(Boolean)
    if (!selectedModuleName.value && targets.value.length > 0) {
      selectedModuleName.value = targets.value[0].moduleName || ''
      await onModuleChanged()
    }
  } catch (e) {
    error.value = e.message || 'Could not load modules'
  } finally {
    loadingTargets.value = false
  }
}

const refreshRepositoryDocuments = async () => {
  loadingDocuments.value = true
  try {
    const response = await fetch(`${API_BASE}/repository/documents?limit=100`)
    if (!response.ok) {
      throw new Error(`Failed loading documents: HTTP ${response.status}`)
    }
    const json = await response.json()
    const rawDocuments = json?.documents || []

    repositoryDocuments.value = rawDocuments
      .filter(Boolean)
      .map((doc) => {
        const nodeId = doc.nodeId || doc.node_id || ''
        const docId = doc.docId || doc.doc_id || ''
        const title = doc.title || doc.doc_id || doc.nodeId || doc.node_id || nodeId || 'Document'
        const size = doc.sizeBytes ?? doc.size_bytes ?? 0
        const drive = doc.drive || ''
        return {
          nodeId,
          docId,
          title,
          label: `${title} (${docId || nodeId})${drive ? ` • drive=${drive}` : ''} • ${formatBytes(size)}`
        }
      })
      .filter((doc) => doc.nodeId)
  } catch (e) {
    statusMessage.value = e.message || 'Failed to load repository documents'
  } finally {
    loadingDocuments.value = false
  }
}

const parseModuleConfig = () => {
  if (!moduleConfigText.value || !moduleConfigText.value.trim()) {
    return {}
  }
  return JSON.parse(moduleConfigText.value)
}

const applyTargetSchemaDefaults = (target) => {
  schemaError.value = ''
  let schema = null
  if (target?.jsonConfigSchema) {
    try {
      schema = JSON.parse(target.jsonConfigSchema)
    } catch (_err) {
      schemaError.value = 'Module schema is not valid JSON'
    }
  }
  if (schema && schema.type === 'object') {
    const currentConfig = parseModuleConfigSafe()
    const merged = deriveDefaultConfigFromSchema(schema, currentConfig)
    moduleConfigText.value = JSON.stringify(merged, null, 2)
  } else if (moduleConfigText.value.trim() === '') {
    moduleConfigText.value = '{}'
  }
}

const parseModuleConfigSafe = () => {
  try {
    return parseModuleConfig()
  } catch (_err) {
    return {}
  }
}

const onModuleChanged = async () => {
  runError.value = ''
  runResult.value = null
  selectedTarget.value = targets.value.find((t) => t.moduleName === selectedModuleName.value) || null

  if (!selectedTarget.value) {
    return
  }

  if (selectedTarget.value.parser) {
    inputMode.value = 'upload'
  } else {
    inputMode.value = 'repository'
  }

  selectedFile.value = null
  selectedRepositoryNodeId.value = ''
  applyTargetSchemaDefaults(selectedTarget.value)
}

const onFileSelected = (event) => {
  const file = event?.target?.files?.[0] || null
  selectedFile.value = file
  if (file) {
    selectedRepositoryNodeId.value = ''
  }
}

const onRepositoryDocumentChanged = () => {
  if (!selectedRepositoryNodeId.value) {
    return
  }
  if (showUploadInput.value) {
    selectedFile.value = null
  }
}

const runTest = async () => {
  if (!selectedTarget.value || !canRun.value) {
    return
  }

  runError.value = ''
  runResult.value = null
  running.value = true

  try {
    const moduleConfig = parseModuleConfig()
    const moduleConfigPayload = JSON.stringify(moduleConfig || {})
    const commonRequest = {
      moduleName: selectedTarget.value.moduleName,
      includeOutputDoc: true,
      moduleConfig: moduleConfigPayload,
      accountId: '',
      pipelineName: 'module-testing-sidecar',
      pipeStepName: 'module-testing-step',
      streamId: '',
      currentHopNumber: 1,
      contextParams: {}
    }

    let response
    if (effectiveMode.value === 'upload') {
      const body = new FormData()
      body.append('moduleName', commonRequest.moduleName)
      body.append('accountId', commonRequest.accountId)
      body.append('includeOutputDoc', 'true')
      body.append('moduleConfigJson', commonRequest.moduleConfig)
      body.append('pipelineName', commonRequest.pipelineName)
      body.append('pipeStepName', commonRequest.pipeStepName)
      body.append('streamId', commonRequest.streamId)
      body.append('currentHopNumber', String(commonRequest.currentHopNumber))
      body.append('contextParamsJson', '{}')
      body.append('file', selectedFile.value)

      response = await fetch(`${API_BASE}/run/upload`, {
        method: 'POST',
        body
      })
    } else {
      const doc = repositoryDocuments.value.find((item) => item.nodeId === selectedRepositoryNodeId.value)
      if (!doc) {
        throw new Error('Select a repository document first')
      }

      const runRequest = {
        moduleName: commonRequest.moduleName,
        repositoryNodeId: doc.nodeId,
        drive: doc.drive || '',
        hydrateBlobFromStorage: false,
        moduleConfig: commonRequest.moduleConfig,
        includeOutputDoc: commonRequest.includeOutputDoc,
        accountId: commonRequest.accountId,
        pipelineName: commonRequest.pipelineName,
        pipeStepName: commonRequest.pipeStepName,
        streamId: commonRequest.streamId,
        currentHopNumber: 1,
        contextParams: {}
      }

      response = await fetch(`${API_BASE}/run/repository`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(runRequest)
      })
    }

    const responseText = await response.text()
    let parsed
    try {
      parsed = responseText ? JSON.parse(responseText) : null
    } catch (_err) {
      parsed = { message: responseText || 'No response body' }
    }

    if (!response.ok) {
      const errMsg = parsed?.message || parsed?.error || responseText || `HTTP ${response.status}`
      throw new Error(errMsg)
    }

    runResult.value = parsed
  } catch (errorObj) {
    runError.value = errorObj?.message || 'Module run failed'
  } finally {
    running.value = false
  }
}

const clearRunResult = () => {
  runResult.value = null
  runError.value = ''
}

const fetchLastError = async () => {
  try {
    const response = await fetch(`${API_BASE}/debug/last-error`)
    const json = await response.json()
    runResult.value = json
    runError.value = ''
  } catch (e) {
    runError.value = e.message || 'Failed to fetch last error'
  }
}

const formatBytes = (bytes) => {
  const value = Number(bytes || 0)
  if (value === 0) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const exponent = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1)
  const amount = value / Math.pow(1024, exponent)
  return `${amount.toFixed(exponent === 0 ? 0 : 1)} ${units[exponent]}`
}

onMounted(async () => {
  await Promise.all([
    loadTargets(),
    refreshRepositoryDocuments()
  ])
})
</script>

<style scoped>
.container {
  font-family: Inter, Arial, sans-serif;
  max-width: 960px;
  margin: 0 auto;
  padding: 16px;
}

.topbar {
  margin-bottom: 16px;
}

.topbar h1 {
  margin: 0 0 8px;
}

.card {
  border: 1px solid #d8d8d8;
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 12px;
  background: #ffffff;
}

.row {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 12px;
}

select,
input[type='file'],
textarea,
button {
  font-size: 14px;
}

select,
input[type='text'],
input[type='file'],
textarea {
  padding: 8px;
  border: 1px solid #b9b9b9;
  border-radius: 6px;
}

textarea {
  width: 100%;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

button {
  border: 1px solid #3f51b5;
  background: #3f51b5;
  color: #fff;
  padding: 8px 14px;
  border-radius: 6px;
  cursor: pointer;
}

button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-secondary {
  background: #757575;
  border-color: #757575;
}

.mode-switch {
  display: flex;
  gap: 16px;
  margin-bottom: 10px;
}

.meta {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-top: 8px;
  font-size: 14px;
}

.warning {
  color: #b00020;
}

.muted {
  color: #5b5b5b;
  font-size: 13px;
}

.result-card {
  max-height: 80vh;
  display: flex;
  flex-direction: column;
}

.result-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 8px;
}

.result-header h2 {
  margin: 0;
}

.result-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.depth-control {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 13px;
}

.depth-control input {
  width: 48px;
  padding: 4px 6px;
  border: 1px solid #b9b9b9;
  border-radius: 4px;
  font-size: 13px;
  text-align: center;
}

.btn-small {
  font-size: 12px;
  padding: 4px 10px;
}

.result-summary {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  padding: 8px 12px;
  background: #f0f4ff;
  border-radius: 6px;
  margin-bottom: 10px;
  font-size: 13px;
}

.summary-chip strong {
  color: #3f51b5;
}

.json-viewer-wrap {
  overflow: auto;
  flex: 1;
  min-height: 0;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  padding: 8px;
  background: #fafafa;
}

.json-viewer-wrap :deep(.vjs-tree) {
  font-size: 13px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}
</style>
