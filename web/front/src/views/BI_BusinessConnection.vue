<script setup>
import * as echarts from 'echarts';
import { onMounted, onUnmounted, nextTick } from 'vue';
import { useCounterStore } from '@/stores/counter';
import KeyMetricCard from '@/components/KeyMetricCard.vue'

const deadline = 32;
const store = useCounterStore();
let myChart = null;

const initChart = async () => {
    const chartDom = document.getElementById("echarts_main");
    if (!chartDom) {
        console.error('找不到图表容器元素');
        return false;
    }

    myChart = echarts.init(chartDom);

    // 绘制图表
    myChart.setOption({
        title: {
            text: 'ECharts 入门示例'
        },
        tooltip: {},
        xAxis: {
            data: ['衬衫', '羊毛衫', '雪纺衫', '裤子', '高跟鞋', '袜子']
        },
        yAxis: {},
        series: [
            {
                name: '销量',
                type: 'bar',
                data: [5, 20, 36, 10, 10, 20]
            }
        ]
    });

    // 监听窗口大小变化
    window.addEventListener('resize', handleResize);
    return true;
};

onMounted(async () => {
    // 先关闭 loading，让容器渲染出来
    store.setChartLoading(false);

    // 等待下一个 tick，确保 DOM 已更新
    await nextTick();

    // 尝试初始化图表
    const success = await initChart();
    if (!success) {
        store.setChartLoading(true); // 如果初始化失败，重新显示 loading 状态
    }
});

const handleResize = () => {
    myChart?.resize();
};

onUnmounted(() => {
    // 移除窗口 resize 监听
    window.removeEventListener('resize', handleResize);
    // 销毁图表实例
    myChart?.dispose();
});
</script>

<template>
    <a-skeleton :loading="store.chartLoading">
        <div class="container">
            <a-row :gutter="[16, 16]">
                <a-col :span="8">
                    <KeyMetricCard
                        :value="deadline"
                        title="流程平均耗时"
                        tooltip-content="hurry up!"
                        unit="小时"
                        description="任务平均耗时："
                    />
                </a-col>
                <a-col :span="8">
                    <a-card>col-8</a-card>
                </a-col>
                <a-col :span="8">
                    <a-card>col-8</a-card>
                </a-col>
            </a-row>
            <a-row :gutter="[16, 16]" style="margin-top: 8px;">
                <a-col :span="24">
                    <a-card>
                        <div id="echarts_main" class="echarts_main"></div>
                    </a-card>
                </a-col>
            </a-row>
        </div>
    </a-skeleton>
</template>

<style>
.container {
    padding: 24px;
}

.tooltip-text {
    color: rgba(0, 0, 0, 0.85);
    /* Ant Design Vue 的标准黑色文本颜色 */
}

.echarts_main {
    width: 100%;
    height: 400px;
    /* 添加最小宽度和最小高度确保容器始终有尺寸 */
    min-width: 200px;
    min-height: 200px;
}
</style>