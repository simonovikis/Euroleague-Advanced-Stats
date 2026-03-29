<script setup>
import { ref, watch, onMounted } from 'vue'
import { useAppStore } from '../stores/appStore'
import apiClient from '../api/client'
import StandingsTable from '../components/StandingsTable.vue'
import SkeletonTable from '../components/SkeletonTable.vue'

const app = useAppStore()

const standings = ref([])
const isLoading = ref(true)
const error = ref(null)

async function fetchStandings(season) {
  isLoading.value = true
  error.value = null
  try {
    const { data } = await apiClient.get(`/api/season/${season}/standings`)
    standings.value = data
  } catch (err) {
    const msg = err.response?.data?.detail ?? err.message
    error.value = `Failed to load standings: ${msg}`
    standings.value = []
  } finally {
    isLoading.value = false
  }
}

onMounted(() => fetchStandings(app.seasonYear))

watch(() => app.seasonYear, (newSeason) => fetchStandings(newSeason))
</script>

<template>
  <div>
    <h1 class="mb-6 text-2xl font-bold tracking-tight text-gray-100">
      Season Overview
      <span class="ml-2 text-lg font-normal text-gray-500">{{ app.seasonYear }}/{{ app.seasonYear + 1 }}</span>
    </h1>

    <!-- Loading state -->
    <SkeletonTable v-if="isLoading" />

    <!-- Error state -->
    <div
      v-else-if="error"
      class="rounded-lg border border-red-500/30 bg-red-500/10 px-6 py-10 text-center"
    >
      <p class="text-red-400">{{ error }}</p>
      <button
        @click="fetchStandings(app.seasonYear)"
        class="mt-4 rounded bg-red-500/20 px-4 py-1.5 text-sm font-medium text-red-300 hover:bg-red-500/30 transition-colors"
      >
        Retry
      </button>
    </div>

    <!-- Data table -->
    <StandingsTable v-else :data="standings" />
  </div>
</template>
