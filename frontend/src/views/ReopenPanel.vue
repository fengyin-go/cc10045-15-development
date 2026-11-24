<template>
  <section class="reopen-panel" data-testid="reopen-panel">
    <h3>告警再开启待办（迟到数据恢复）</h3>
    <p class="panel-desc">
      迟到超限采样已关联回已结案业务区间，原关闭证据随记录保留；领取后生成的新调查带齐新证据与上一班关闭依据。
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>记录</th>
          <th>告警规则</th>
          <th>监控指标</th>
          <th>业务区间</th>
          <th>迟到采样</th>
          <th>上一班关闭依据</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in reopens" :key="item.id" :data-testid="`reopen-${item.id}`">
          <td>#{{ item.id }}</td>
          <td>{{ item.alert_rule_name }}</td>
          <td>{{ item.metric_name }}</td>
          <td>{{ item.interval_key }}</td>
          <td>采样 {{ item.metric_value_id }}（{{ item.metric_value }}）</td>
          <td>
            <span v-if="item.closure_evidence && item.closure_evidence.analysis_id">
              旧调查 #{{ item.closure_evidence.analysis_id }}：{{ item.closure_evidence.conclusion || '—' }}
            </span>
            <span v-else>—</span>
          </td>
          <td>{{ item.status }}<span v-if="item.claimed_by">（{{ item.claimed_by }}）</span></td>
          <td class="row-actions">
            <button
              v-if="item.status === '待领取'"
              class="link"
              type="button"
              :data-testid="`claim-${item.id}`"
              @click="claim(item)"
            >
              领取
            </button>
            <button
              v-if="item.analysis_id"
              class="link"
              type="button"
              :data-testid="`view-samples-${item.analysis_id}`"
              @click="viewSamples(item.analysis_id)"
            >
              查看本次归因采样
            </button>
          </td>
        </tr>
        <tr v-if="!reopens.length">
          <td colspan="8" class="empty-state">暂无告警再开启待办</td>
        </tr>
      </tbody>
    </table>

    <div class="sample-viewer">
      <h4>调查采样查看（本次归因与旧档分列）</h4>
      <form class="filter-bar" @submit.prevent="viewSamples(selectedEntryId)">
        <label class="filter-item">
          <span>调查编号</span>
          <input v-model.number="selectedEntryId" data-testid="sample-entry-id" placeholder="输入归因分析器 id" />
        </label>
        <button class="btn" data-testid="load-samples" type="submit">查看采样</button>
      </form>

      <div v-if="samples" class="sample-columns" data-testid="sample-columns">
        <div class="sample-col">
          <h5>本次归因 #{{ samples.entry_id }}（{{ samples.status }}）使用的指标采样</h5>
          <table class="data-table">
            <thead>
              <tr><th>采样 id</th><th>采样值</th><th>采样时间</th></tr>
            </thead>
            <tbody>
              <tr v-for="s in samples.samples" :key="s.id" :data-testid="`current-sample-${s.id}`">
                <td>{{ s.id }}</td>
                <td>{{ s.snapshot_value }}</td>
                <td>{{ s.snapshot_time }}</td>
              </tr>
              <tr v-if="!samples.samples.length"><td colspan="3" class="empty-state">暂无采样记录</td></tr>
            </tbody>
          </table>
        </div>
        <div v-if="samples.predecessor" class="sample-col">
          <h5>旧档 #{{ samples.predecessor.entry_id }}（{{ samples.predecessor.status }}）使用的指标采样</h5>
          <table class="data-table">
            <thead>
              <tr><th>采样 id</th><th>采样值</th><th>采样时间</th></tr>
            </thead>
            <tbody>
              <tr v-for="s in samples.predecessor.samples" :key="s.id" :data-testid="`predecessor-sample-${s.id}`">
                <td>{{ s.id }}</td>
                <td>{{ s.snapshot_value }}</td>
                <td>{{ s.snapshot_time }}</td>
              </tr>
              <tr v-if="!samples.predecessor.samples.length"><td colspan="3" class="empty-state">暂无采样记录</td></tr>
            </tbody>
          </table>
          <p class="panel-desc">上一班结论：{{ samples.predecessor.conclusion || '—' }}</p>
        </div>
      </div>
    </div>

    <p v-if="message" class="panel-message">{{ message }}</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  claimReopen,
  listReopens,
  loadInvestigationSamples,
  type InvestigationSamples,
  type ReopenRecord,
} from '@/api/backend-service'

const reopens = ref<ReopenRecord[]>([])
const samples = ref<InvestigationSamples | null>(null)
const selectedEntryId = ref<number | null>(null)
const message = ref('')

async function reload() {
  try {
    const payload = await listReopens()
    reopens.value = payload.items
  } catch (error) {
    message.value = error instanceof Error ? error.message : '待办读取失败'
  }
}

async function claim(item: ReopenRecord) {
  message.value = ''
  try {
    const result = await claimReopen(item.id, '下一班')
    message.value = result.message
    await reload()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '领取失败'
  }
}

async function viewSamples(entryId: number | null) {
  if (!entryId) return
  message.value = ''
  try {
    samples.value = await loadInvestigationSamples(entryId)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '采样读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.reopen-panel {
  margin-top: 24px;
  border-top: 1px solid #d8dee9;
  padding-top: 16px;
}
.panel-desc {
  color: #5b6472;
  font-size: 13px;
}
.sample-viewer {
  margin-top: 16px;
}
.sample-columns {
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
}
.sample-col {
  flex: 1 1 320px;
}
.panel-message {
  color: #1a6ef5;
}
</style>
