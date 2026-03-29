<script setup>
import { ref, onMounted, watch, onBeforeUnmount } from 'vue'
import Plotly from 'plotly.js-dist-min'

const props = defineProps({
  chartData: {
    type: Object,
    required: true,
    // { r: number[], theta: string[] }
  },
  color: {
    type: String,
    default: '#6366f1',
  },
  teamName: {
    type: String,
    default: 'Team',
  },
})

const chartEl = ref(null)

function hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return `rgba(${r},${g},${b},${alpha})`
}

function render() {
  if (!chartEl.value || !props.chartData?.r?.length) return

  const r = [...props.chartData.r, props.chartData.r[0]]
  const theta = [...props.chartData.theta, props.chartData.theta[0]]

  const traces = [
    // League average reference
    {
      type: 'scatterpolar',
      r: Array(theta.length).fill(50),
      theta,
      mode: 'lines',
      line: { color: '#4b5563', width: 1, dash: 'dash' },
      name: 'League Avg',
      fill: 'none',
      hoverinfo: 'skip',
    },
    // Team trace
    {
      type: 'scatterpolar',
      r,
      theta,
      fill: 'toself',
      fillcolor: hexToRgba(props.color, 0.15),
      line: { color: props.color, width: 2.5 },
      name: props.teamName,
      marker: { size: 5, color: props.color },
    },
  ]

  const layout = {
    polar: {
      bgcolor: 'transparent',
      radialaxis: {
        visible: true,
        range: [0, 100],
        tickvals: [25, 50, 75, 100],
        tickfont: { size: 10, color: '#6b7280' },
        gridcolor: '#374151',
        linecolor: '#374151',
      },
      angularaxis: {
        tickfont: { size: 11, color: '#d1d5db' },
        gridcolor: '#374151',
        linecolor: '#374151',
      },
    },
    showlegend: true,
    legend: {
      font: { color: '#9ca3af', size: 11 },
      bgcolor: 'transparent',
      x: 0.5,
      xanchor: 'center',
      y: -0.15,
      orientation: 'h',
    },
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    margin: { t: 30, b: 40, l: 50, r: 50 },
    font: { color: '#d1d5db' },
  }

  const config = {
    displayModeBar: false,
    responsive: true,
  }

  Plotly.react(chartEl.value, traces, layout, config)
}

onMounted(render)

watch(() => [props.chartData, props.color, props.teamName], render, { deep: true })

onBeforeUnmount(() => {
  if (chartEl.value) Plotly.purge(chartEl.value)
})
</script>

<template>
  <div ref="chartEl" class="w-full h-full min-h-[380px]"></div>
</template>
