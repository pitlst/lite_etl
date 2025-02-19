import './assets/main.css'
import 'ant-design-vue/dist/reset.css';

import Antd from 'ant-design-vue';

import { createApp } from 'vue'
import { createPinia } from 'pinia'

import App from './App.vue'
import router from './router'

const app = createApp(App)
const pinia = createPinia()

app.use(Antd)
app.use(pinia)
app.use(router)

app.mount('#app')
