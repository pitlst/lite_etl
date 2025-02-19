import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: '/',
            name: 'home',
            component: () => import('../views/HomeView.vue'),
        },
        {
            path: '/bi/business-connection',
            name: '业联系统看板',
            component: () => import('../views/BI_BusinessConnection.vue'),
        },
    ],
})

export default router
