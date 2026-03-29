<script setup>
import { ref, computed, watch, onMounted } from 'vue'
import { useAppStore } from '../stores/appStore'
import apiClient from '../api/client'
import RadarChart from '../components/RadarChart.vue'

const app = useAppStore()

const allTeams = ref([])         // full DNA array from API
const selectedCode = ref(null)
const isLoading = ref(true)
const error = ref(null)

// Derived: currently selected team object
const selectedTeam = computed(() =>
  allTeams.value.find((t) => t.team_code === selectedCode.value) ?? null
)

const chartData = computed(() => {
  if (!selectedTeam.value) return null
  return {
    r: selectedTeam.value.percentiles,
    theta: selectedTeam.value.categories,
  }
})

// Group teams by cluster for the sidebar
const clusters = computed(() => {
  const map = {}
  for (const t of allTeams.value) {
    const name = t.cluster_name
    if (!map[name]) map[name] = { name, color: t.cluster_color, teams: [] }
    map[name].teams.push(t)
  }
  return Object.values(map)
})

async function fetchDna(season) {
  isLoading.value = true
  error.value = null
  try {
    const { data } = await apiClient.get('/api/teams/dna', {
      params: { season },
    })
    allTeams.value = data.teams
    // Auto-select first team if nothing selected
    if (!selectedCode.value || !data.teams.some((t) => t.team_code === selectedCode.value)) {
      selectedCode.value = data.teams[0]?.team_code ?? null
    }
  } catch (err) {
    const msg = err.response?.data?.detail ?? err.message
    error.value = `Failed to load Team DNA: ${msg}`
    allTeams.value = []
  } finally {
    isLoading.value = false
  }
}

onMounted(() => fetchDna(app.seasonYear))
watch(() => app.seasonYear, (s) => fetchDna(s))
</script>

<template>
  <div>
    <h1 class="mb-6 text-2xl font-bold tracking-tight text-gray-100">
      Team DNA
      <span class="ml-2 text-lg font-normal text-gray-500">{{ app.seasonYear }}/{{ app.seasonYear + 1 }}</span>
    </h1>

    <!-- Loading skeleton -->
    <div v-if="isLoading" class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div class="lg:col-span-2 flex items-center justify-center rounded-lg border border-gray-700 bg-gray-800/40 p-10">
        <div class="h-64 w-64 rounded-full bg-gray-700/50 animate-pulse"></div>
      </div>
      <div class="space-y-4">
        <div class="h-10 w-full rounded bg-gray-700/50 animate-pulse"></div>
        <div v-for="i in 4" :key="i" class="h-20 rounded-lg bg-gray-700/30 animate-pulse"></div>
      </div>
    </div>

    <!-- Error state -->
    <div
      v-else-if="error"
      class="rounded-lg border border-red-500/30 bg-red-500/10 px-6 py-10 text-center"
    >
      <p class="text-red-400">{{ error }}</p>
      <button
        @click="fetchDna(app.seasonYear)"
        class="mt-4 rounded bg-red-500/20 px-4 py-1.5 text-sm font-medium text-red-300 hover:bg-red-500/30 transition-colors"
      >
        Retry
      </button>
    </div>

    <!-- Main content -->
    <div v-else class="grid grid-cols-1 lg:grid-cols-3 gap-6">

      <!-- Radar chart panel -->
      <div class="lg:col-span-2 rounded-lg border border-gray-700 bg-gray-800/40 p-4">
        <!-- Team selector -->
        <div class="mb-4 flex items-center gap-3">
          <label for="team-select" class="text-sm text-gray-400">Team</label>
          <select
            id="team-select"
            v-model="selectedCode"
            class="rounded bg-gray-800 px-3 py-1.5 text-sm text-gray-200 border border-gray-600 focus:outline-none focus:ring-1 focus:ring-orange-400"
          >
            <option v-for="t in allTeams" :key="t.team_code" :value="t.team_code">
              {{ t.team_code }}
            </option>
          </select>

          <!-- Cluster badge -->
          <span
            v-if="selectedTeam"
            class="inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium"
            :style="{ backgroundColor: selectedTeam.cluster_color + '20', color: selectedTeam.cluster_color }"
          >
            <span class="inline-block h-1.5 w-1.5 rounded-full" :style="{ backgroundColor: selectedTeam.cluster_color }"></span>
            {{ selectedTeam.cluster_name }}
          </span>
        </div>

        <RadarChart
          v-if="chartData"
          :chart-data="chartData"
          :color="selectedTeam?.cluster_color ?? '#6366f1'"
          :team-name="selectedTeam?.team_code ?? 'Team'"
        />
      </div>

      <!-- Cluster sidebar -->
      <div class="space-y-4">
        <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-400">Clusters</h2>

        <div
          v-for="cluster in clusters"
          :key="cluster.name"
          class="rounded-lg border border-gray-700 bg-gray-800/30 p-3"
        >
          <div class="flex items-center gap-2 mb-2">
            <span class="inline-block h-2.5 w-2.5 rounded-full" :style="{ backgroundColor: cluster.color }"></span>
            <span class="text-sm font-medium text-gray-200">{{ cluster.name }}</span>
          </div>
          <div class="flex flex-wrap gap-1.5">
            <button
              v-for="t in cluster.teams"
              :key="t.team_code"
              @click="selectedCode = t.team_code"
              class="rounded px-2 py-0.5 text-xs font-medium transition-colors"
              :class="selectedCode === t.team_code
                ? 'bg-gray-600 text-gray-100'
                : 'bg-gray-700/50 text-gray-400 hover:bg-gray-700 hover:text-gray-200'"
            >
              {{ t.team_code }}
            </button>
          </div>
        </div>

        <!-- Raw stats for selected team -->
        <div v-if="selectedTeam" class="rounded-lg border border-gray-700 bg-gray-800/30 p-3">
          <h3 class="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-2">Raw Stats</h3>
          <dl class="grid grid-cols-2 gap-x-4 gap-y-1.5 text-sm">
            <template v-for="(label, key) in { efg_pct: 'eFG%', tov_pct: 'TOV%', orb_pct: 'ORB%', ft_rate: 'FT Rate', pace: 'Pace', three_pt_rate: '3PA Rate' }" :key="key">
              <dt class="text-gray-500">{{ label }}</dt>
              <dd class="text-right tabular-nums text-gray-300">
                {{ selectedTeam.raw[key] != null ? (key === 'pace' ? selectedTeam.raw[key].toFixed(1) : (selectedTeam.raw[key] * 100).toFixed(1) + '%') : '—' }}
              </dd>
            </template>
          </dl>
        </div>
      </div>
    </div>
  </div>
</template>
