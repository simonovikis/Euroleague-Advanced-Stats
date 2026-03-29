import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'SeasonOverview',
    component: () => import('../views/SeasonOverview.vue'),
    alias: '/standings',
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
