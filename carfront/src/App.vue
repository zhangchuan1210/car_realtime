// 示例：使用Vue 3重写index.jsp
<template>
  <div>
    <h1>Welcome to the Dashboard</h1>
    <div class="sections">
      <AddressSummary />
      <CarCount />
      <OilAverage />
      <PriceRange />
      <ScoreAverage />
      <ScoreRange />
    </div>
  </div>
</template>

<script>
import AddressSummary from './components/AddressSummary.vue';
import CarCount from './components/CarCount.vue';
import OilAverage from './components/OilAverage.vue';
import PriceRange from './components/PriceRange.vue';
import ScoreAverage from './components/ScoreAverage.vue';
import ScoreRange from './components/ScoreRange.vue';

export default {
  name: 'App',
  components: {
    AddressSummary,
    CarCount,
    OilAverage,
    PriceRange,
    ScoreAverage,
    ScoreRange,
  }
};
</script>

<style scoped>
/* 可以根据需要添加样式 */
.sections {
  display: flex;
  flex-direction: column;
  gap: 20px;
  margin-top: 20px;
}
</style>

