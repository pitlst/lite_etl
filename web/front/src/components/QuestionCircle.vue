<template>
    <a-card :bodyStyle="{ padding: '12px 24px' }">
        <a-typography-title :level="5" style="margin-bottom: 0; letter-spacing: 0.5px">
            <span>{{ title }}</span>
            <a-tooltip color="white" placement="right">
                <template #title>
                    <span class="tooltip-text">{{ tooltipContent }}</span>
                </template>
                <QuestionCircleOutlined style="margin-left: 5px" />
            </a-tooltip>
        </a-typography-title>
        <div style="display: flex; align-items: baseline;">
            <a-statistic :value="value" :value-style="{ fontSize: '38px', fontWeight: 600, letterSpacing: '0.5px' }" />
            <span style="font-size: 14px; letter-spacing: 0.3px; margin-left: 4px">{{ unit }}</span>
        </div>
        <a-typography-paragraph type="secondary" style="margin-bottom: 0; letter-spacing: 0.3px">{{ description }}</a-typography-paragraph>
    </a-card>
</template>

<script setup>
import { QuestionCircleOutlined } from '@ant-design/icons-vue'

// 定义 props
const props = defineProps({
    title: {
        type: String,
        default: '流程平均耗时'
    },
    tooltipContent: {
        type: String,
        default: 'hurry up!'
    },
    value: {
        type: Number,
        required: true
    },
    unit: {
        type: String,
        default: '小时'
    },
    description: {
        type: String,
        default: '任务平均耗时：'
    }
})
</script>

<style scoped>
.tooltip-text {
    color: rgba(0, 0, 0, 0.85);
}
</style>