import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'SeasonOverview',
    component: () => import('../views/SeasonOverview.vue'),
    alias: '/standings',
  },
  {
    path: '/dna',
    name: 'TeamDNA',
    component: () => import('../views/TeamDNA.vue'),
  },
  {
    path: '/predictor',
    name: 'PlayoffPredictor',
    component: () => import('../views/PlayoffPredictor.vue'),
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
