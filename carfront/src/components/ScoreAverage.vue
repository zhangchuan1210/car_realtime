<template>
  <div>
    <h2>Score Average</h2>
    <div v-if="loading">Loading...</div>
    <div v-else-if="error">Error: {{ error }}</div>
    <div v-else>
      <p>Average Score: {{ scoreAvg }}</p>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import axios from 'axios';

export default {
  name: 'ScoreAverage',
  setup() {
    const scoreAvg = ref(null);
    const loading = ref(true);
    const error = ref(null);

    const fetchData = async () => {
      try {
        const response = await axios.get('/api/score/average');
        scoreAvg.value = response.data.average;
      } catch (err) {
        error.value = err.message;
      } finally {
        loading.value = false;
      }
    };

    onMounted(() => {
      fetchData();
    });

    return {
      scoreAvg,
      loading,
      error,
    };
  }
};
</script>