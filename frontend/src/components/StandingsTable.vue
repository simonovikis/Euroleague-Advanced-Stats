<script setup>
defineProps({
  data: {
    type: Array,
    required: true,
  },
})

function fmt(val, decimals = 1) {
  if (val == null) return '—'
  return Number(val).toFixed(decimals)
}

function tierClass(rank) {
  if (rank <= 6) return 'border-l-emerald-400'
  if (rank <= 10) return 'border-l-amber-400'
  return 'border-l-transparent'
}

function tierBg(rank) {
  if (rank <= 6) return 'bg-emerald-400/5'
  if (rank <= 10) return 'bg-amber-400/5'
  return ''
}
</script>

<template>
  <div class="overflow-x-auto rounded-lg border border-gray-700">
    <table class="w-full text-sm text-left">
      <!-- Sticky header -->
      <thead class="sticky top-0 z-10 bg-gray-800 text-xs uppercase tracking-wider text-gray-400">
        <tr>
          <th class="px-4 py-3 w-10">#</th>
          <th class="px-4 py-3">Team</th>
          <th class="px-4 py-3 text-center">GP</th>
          <th class="px-4 py-3 text-center">Pace</th>
          <th class="px-4 py-3 text-center">ORtg</th>
          <th class="px-4 py-3 text-center">DRtg</th>
          <th class="px-4 py-3 text-center">Net Rtg</th>
        </tr>
      </thead>

      <tbody class="divide-y divide-gray-700/50">
        <tr
          v-for="(team, idx) in data"
          :key="team.team_code"
          class="border-l-2 transition-colors hover:bg-gray-800/60"
          :class="[tierClass(idx + 1), tierBg(idx + 1)]"
        >
          <!-- Rank -->
          <td class="px-4 py-3 font-medium text-gray-400">{{ idx + 1 }}</td>

          <!-- Team name + logo -->
          <td class="px-4 py-3">
            <div class="flex items-center gap-2.5">
              <img
                v-if="team.logo_url"
                :src="team.logo_url"
                :alt="team.team_name"
                class="h-6 w-6 object-contain"
                loading="lazy"
              />
              <div class="h-6 w-6 rounded bg-gray-700 flex items-center justify-center text-[10px] font-bold text-gray-400" v-else>
                {{ team.team_code?.slice(0, 2) }}
              </div>
              <span class="font-medium text-gray-100">{{ team.team_name }}</span>
              <span class="text-xs text-gray-500">{{ team.team_code }}</span>
            </div>
          </td>

          <!-- Stats -->
          <td class="px-4 py-3 text-center tabular-nums text-gray-300">{{ team.games }}</td>
          <td class="px-4 py-3 text-center tabular-nums text-gray-300">{{ fmt(team.pace, 1) }}</td>
          <td class="px-4 py-3 text-center tabular-nums text-gray-300">{{ fmt(team.ortg, 1) }}</td>
          <td class="px-4 py-3 text-center tabular-nums text-gray-300">{{ fmt(team.drtg, 1) }}</td>
          <td class="px-4 py-3 text-center tabular-nums font-semibold"
              :class="team.net_rtg > 0 ? 'text-emerald-400' : team.net_rtg < 0 ? 'text-red-400' : 'text-gray-300'"
          >
            {{ team.net_rtg > 0 ? '+' : '' }}{{ fmt(team.net_rtg, 1) }}
          </td>
        </tr>
      </tbody>
    </table>

    <!-- Legend -->
    <div class="flex items-center gap-6 border-t border-gray-700 bg-gray-800/40 px-4 py-2 text-xs text-gray-500">
      <span class="flex items-center gap-1.5">
        <span class="inline-block h-2.5 w-2.5 rounded-sm bg-emerald-400"></span> Playoffs (1-6)
      </span>
      <span class="flex items-center gap-1.5">
        <span class="inline-block h-2.5 w-2.5 rounded-sm bg-amber-400"></span> Play-In (7-10)
      </span>
    </div>
  </div>
</template>
