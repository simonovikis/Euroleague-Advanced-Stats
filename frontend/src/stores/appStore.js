import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useAppStore = defineStore('app', () => {
  const seasonYear = ref(new Date().getFullYear())

  function setSeasonYear(year) {
    seasonYear.value = year
  }

  return { seasonYear, setSeasonYear }
})
