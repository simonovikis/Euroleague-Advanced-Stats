<script setup>
import { ref, watch, onMounted } from 'vue'
import { useAppStore } from '../stores/appStore'
import apiClient from '../api/client'

const app = useAppStore()

const simulationsData = ref([])
const isSimulating = ref(false)
const error = ref(null)
const seasonComplete = ref(false)

async function runSimulation(season) {
  isSimulating.value = true
  error.value = null
  try {
    const { data } = await apiClient.get(`/api/season/${season}/monte-carlo`, {
      params: { runs: 10000 },
    })
    simulationsData.value = data
    seasonComplete.value = data.length > 0 && data[0].games_simulated === 0
  } catch (err) {
    const msg = err.response?.data?.detail ?? err.message
    error.value = `Simulation failed: ${msg}`
    simulationsData.value = []
  } finally {
    isSimulating.value = false
  }
}

onMounted(() => runSimulation(app.seasonYear))
watch(() => app.seasonYear, (s) => runSimulation(s))

function barColor(type) {
  switch (type) {
    case 'top4': return 'bg-amber-400'
    case 'top6': return 'bg-blue-500'
    case 'top10': return 'bg-gray-400'
    default: return 'bg-gray-500'
  }
}

function barTrack(type) {
  switch (type) {
    case 'top4': return 'bg-amber-400/10'
    case 'top6': return 'bg-blue-500/10'
    case 'top10': return 'bg-gray-400/10'
    default: return 'bg-gray-700'
  }
}

function textColor(type) {
  switch (type) {
    case 'top4': return 'text-amber-400'
    case 'top6': return 'text-blue-400'
    case 'top10': return 'text-gray-400'
    default: return 'text-gray-500'
  }
}
</script>

<template>
  <div>
    <div class="mb-6 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
      <h1 class="text-2xl font-bold tracking-tight text-gray-100">
        Playoff Predictor
        <span class="ml-2 text-lg font-normal text-gray-500">{{ app.seasonYear }}/{{ app.seasonYear + 1 }}</span>
      </h1>
      <button
        @click="runSimulation(app.seasonYear)"
        :disabled="isSimulating"
        class="inline-flex items-center gap-2 rounded-lg px-5 py-2.5 text-sm font-semibold transition-all
               bg-gradient-to-r from-orange-500 to-amber-500 text-gray-900
               hover:from-orange-400 hover:to-amber-400 hover:shadow-lg hover:shadow-orange-500/20
               disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:shadow-none"
      >
        <svg v-if="isSimulating" class="h-4 w-4 animate-spin" viewBox="0 0 24 24" fill="none">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
        </svg>
        <svg v-else xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clip-rule="evenodd"/>
        </svg>
        {{ isSimulating ? 'Simulating...' : 'Run 10,000 Simulations' }}
      </button>
    </div>

    <!-- Simulating state -->
    <div v-if="isSimulating" class="flex flex-col items-center justify-center py-24">
      <!-- Glowing spinner ring -->
      <div class="relative h-28 w-28 mb-8">
        <div class="absolute inset-0 rounded-full border-4 border-orange-500/20"></div>
        <div class="absolute inset-0 rounded-full border-4 border-transparent border-t-orange-400 animate-spin"></div>
        <div class="absolute inset-3 rounded-full border-4 border-transparent border-t-amber-400 animate-spin" style="animation-direction: reverse; animation-duration: 1.5s;"></div>
        <div class="absolute inset-0 rounded-full shadow-[0_0_40px_rgba(251,146,60,0.3)]"></div>
      </div>
      <p class="text-lg font-medium text-gray-300 mb-1">Crunching the numbers...</p>
      <p class="text-sm text-gray-500">Running 10,000 alternate realities</p>
    </div>

    <!-- Error state -->
    <div
      v-else-if="error"
      class="rounded-lg border border-red-500/30 bg-red-500/10 px-6 py-10 text-center"
    >
      <p class="text-red-400">{{ error }}</p>
      <button
        @click="runSimulation(app.seasonYear)"
        class="mt-4 rounded bg-red-500/20 px-4 py-1.5 text-sm font-medium text-red-300 hover:bg-red-500/30 transition-colors"
      >
        Retry
      </button>
    </div>

    <!-- Results -->
    <div v-else-if="simulationsData.length">
      <!-- Season state banner -->
      <div v-if="seasonComplete" class="mb-4 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-2.5 text-sm text-emerald-400">
        Season complete — these are the final standings.
      </div>

      <!-- Results table -->
      <div class="overflow-x-auto rounded-lg border border-gray-700">
        <table class="w-full text-sm text-left">
          <thead class="sticky top-0 z-10 bg-gray-800 text-xs uppercase tracking-wider text-gray-400">
            <tr>
              <th class="px-4 py-3 w-10">#</th>
              <th class="px-4 py-3">Team</th>
              <th class="px-4 py-3 text-center">Record</th>
              <th class="px-4 py-3 text-center">Proj. W-L</th>
              <th class="px-4 py-3 w-44">
                <div class="flex items-center gap-1.5">
                  <span class="inline-block h-2 w-2 rounded-sm bg-amber-400"></span>Top 4
                </div>
              </th>
              <th class="px-4 py-3 w-44">
                <div class="flex items-center gap-1.5">
                  <span class="inline-block h-2 w-2 rounded-sm bg-blue-500"></span>Top 6
                </div>
              </th>
              <th class="px-4 py-3 w-44">
                <div class="flex items-center gap-1.5">
                  <span class="inline-block h-2 w-2 rounded-sm bg-gray-400"></span>Top 10
                </div>
              </th>
              <th class="px-4 py-3 text-center">#1 Seed</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-700/50">
            <tr
              v-for="team in simulationsData"
              :key="team.team_code"
              class="transition-colors hover:bg-gray-800/60"
              :class="{
                'bg-amber-400/5': team.proj_rank <= 4,
                'bg-blue-500/5': team.proj_rank > 4 && team.proj_rank <= 6,
                'bg-gray-500/5': team.proj_rank > 6 && team.proj_rank <= 10,
              }"
            >
              <!-- Rank -->
              <td class="px-4 py-3 font-medium text-gray-400">{{ team.proj_rank }}</td>

              <!-- Team code -->
              <td class="px-4 py-3 font-medium text-gray-100">{{ team.team_code }}</td>

              <!-- Current record -->
              <td class="px-4 py-3 text-center tabular-nums text-gray-300">
                {{ team.current_wins }}-{{ team.current_losses }}
              </td>

              <!-- Projected record -->
              <td class="px-4 py-3 text-center tabular-nums text-gray-300">
                {{ team.avg_wins }}-{{ team.avg_losses }}
              </td>

              <!-- Top 4 bar -->
              <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                  <div class="flex-1 rounded-full h-2" :class="barTrack('top4')">
                    <div
                      class="h-2 rounded-full transition-all duration-700"
                      :class="barColor('top4')"
                      :style="{ width: (team.make_top_4_pct ?? 0) + '%' }"
                    ></div>
                  </div>
                  <span class="w-10 text-right text-xs tabular-nums font-medium" :class="textColor('top4')">
                    {{ team.make_top_4_pct != null ? team.make_top_4_pct.toFixed(0) : '—' }}%
                  </span>
                </div>
              </td>

              <!-- Top 6 bar -->
              <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                  <div class="flex-1 rounded-full h-2" :class="barTrack('top6')">
                    <div
                      class="h-2 rounded-full transition-all duration-700"
                      :class="barColor('top6')"
                      :style="{ width: (team.make_top_6_pct ?? 0) + '%' }"
                    ></div>
                  </div>
                  <span class="w-10 text-right text-xs tabular-nums font-medium" :class="textColor('top6')">
                    {{ team.make_top_6_pct != null ? team.make_top_6_pct.toFixed(0) : '—' }}%
                  </span>
                </div>
              </td>

              <!-- Top 10 bar -->
              <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                  <div class="flex-1 rounded-full h-2" :class="barTrack('top10')">
                    <div
                      class="h-2 rounded-full transition-all duration-700"
                      :class="barColor('top10')"
                      :style="{ width: (team.make_top_10_pct ?? 0) + '%' }"
                    ></div>
                  </div>
                  <span class="w-10 text-right text-xs tabular-nums font-medium" :class="textColor('top10')">
                    {{ team.make_top_10_pct != null ? team.make_top_10_pct.toFixed(0) : '—' }}%
                  </span>
                </div>
              </td>

              <!-- #1 Seed -->
              <td class="px-4 py-3 text-center tabular-nums text-sm"
                  :class="team.win_rs_pct > 20 ? 'text-orange-400 font-semibold' : 'text-gray-500'"
              >
                {{ team.win_rs_pct != null ? team.win_rs_pct.toFixed(1) : '—' }}%
              </td>
            </tr>
          </tbody>
        </table>

        <!-- Legend -->
        <div class="flex items-center gap-6 border-t border-gray-700 bg-gray-800/40 px-4 py-2 text-xs text-gray-500">
          <span class="flex items-center gap-1.5">
            <span class="inline-block h-2.5 w-2.5 rounded-sm bg-amber-400"></span> Home Court (1-4)
          </span>
          <span class="flex items-center gap-1.5">
            <span class="inline-block h-2.5 w-2.5 rounded-sm bg-blue-500"></span> Playoffs (5-6)
          </span>
          <span class="flex items-center gap-1.5">
            <span class="inline-block h-2.5 w-2.5 rounded-sm bg-gray-400"></span> Play-In (7-10)
          </span>
        </div>
      </div>
    </div>
  </div>
</template>
