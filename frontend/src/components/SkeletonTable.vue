<template>
  <div class="overflow-hidden rounded-lg border border-gray-700">
    <!-- Header skeleton -->
    <div class="bg-gray-800 px-4 py-3">
      <div class="flex gap-6">
        <div class="h-3 w-6 rounded bg-gray-700 animate-pulse"></div>
        <div class="h-3 w-32 rounded bg-gray-700 animate-pulse"></div>
        <div class="h-3 w-10 rounded bg-gray-700 animate-pulse"></div>
        <div class="h-3 w-10 rounded bg-gray-700 animate-pulse"></div>
        <div class="h-3 w-10 rounded bg-gray-700 animate-pulse"></div>
        <div class="h-3 w-10 rounded bg-gray-700 animate-pulse"></div>
        <div class="h-3 w-14 rounded bg-gray-700 animate-pulse"></div>
      </div>
    </div>

    <!-- Row skeletons -->
    <div class="divide-y divide-gray-700/50">
      <div
        v-for="i in rows"
        :key="i"
        class="flex items-center gap-6 px-4 py-3.5"
      >
        <div class="h-3 w-4 rounded bg-gray-700/60 animate-pulse"></div>
        <div class="flex items-center gap-2.5">
          <div class="h-6 w-6 rounded-full bg-gray-700/60 animate-pulse"></div>
          <div class="h-3 rounded bg-gray-700/60 animate-pulse" :style="{ width: randomWidth(i) }"></div>
        </div>
        <div class="ml-auto flex gap-8">
          <div class="h-3 w-8 rounded bg-gray-700/60 animate-pulse" v-for="j in 5" :key="j"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
defineProps({
  rows: {
    type: Number,
    default: 18,
  },
})

function randomWidth(seed) {
  const widths = ['6rem', '7rem', '8rem', '9rem', '7.5rem', '6.5rem']
  return widths[seed % widths.length]
}
</script>
