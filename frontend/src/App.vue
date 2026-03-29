<script setup>
import { useAppStore } from './stores/appStore'
import { RouterLink } from 'vue-router'

const app = useAppStore()
</script>

<template>
  <div class="min-h-screen bg-gray-900 text-gray-100">
    <!-- Top Navbar -->
    <nav class="sticky top-0 z-50 border-b border-gray-700 bg-gray-900/80 backdrop-blur">
      <div class="mx-auto flex h-14 max-w-7xl items-center justify-between px-4">
        <div class="flex items-center gap-6">
          <span class="text-lg font-bold tracking-tight text-orange-400">
            Euroleague Analytics
          </span>
          <div class="hidden sm:flex items-center gap-1 text-sm">
            <RouterLink
              to="/"
              class="rounded px-3 py-1.5 transition-colors hover:bg-gray-800 hover:text-gray-100"
              active-class="bg-gray-800 text-gray-100"
              inactive-class="text-gray-400"
            >
              Standings
            </RouterLink>
            <RouterLink
              to="/dna"
              class="rounded px-3 py-1.5 transition-colors hover:bg-gray-800 hover:text-gray-100"
              active-class="bg-gray-800 text-gray-100"
              inactive-class="text-gray-400"
            >
              Team DNA
            </RouterLink>
            <RouterLink
              to="/predictor"
              class="rounded px-3 py-1.5 transition-colors hover:bg-gray-800 hover:text-gray-100"
              active-class="bg-gray-800 text-gray-100"
              inactive-class="text-gray-400"
            >
              Predictor
            </RouterLink>
          </div>
        </div>

        <div class="flex items-center gap-3">
          <label for="season" class="text-sm text-gray-400">Season</label>
          <select
            id="season"
            :value="app.seasonYear"
            @change="app.setSeasonYear(Number($event.target.value))"
            class="rounded bg-gray-800 px-2 py-1 text-sm text-gray-200 border border-gray-600 focus:outline-none focus:ring-1 focus:ring-orange-400"
          >
            <option v-for="y in 5" :key="app.seasonYear - y + 1" :value="app.seasonYear - y + 1">
              {{ app.seasonYear - y + 1 }}
            </option>
          </select>
        </div>
      </div>
    </nav>

    <!-- Page content -->
    <main class="mx-auto max-w-7xl px-4 py-6">
      <router-view />
    </main>
  </div>
</template>
