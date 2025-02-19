<template>
    <a-tooltip color=white placement="right">
        <template #title>
            <span class="tooltip-text">hurry up!</span>
        </template>
        <QuestionCircleOutlined style="margin-left: 5px" />
    </a-tooltip>
</template>

<script setup>
import { QuestionCircleOutlined } from '@ant-design/icons-vue'
</script>

<style scoped>
.tooltip-text {
    color: rgba(0, 0, 0, 0.85);  /* Ant Design Vue 的标准黑色文本颜色 */
}
</style>