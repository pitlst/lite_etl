import { ref, computed } from 'vue'
import { defineStore } from 'pinia'

export const useCounterStore = defineStore('counter', () => {
    const chartLoading = ref(true)
    function setChartLoading(status) {
        chartLoading.value = status
    }

    return { 
        chartLoading,
        setChartLoading
    }
})
