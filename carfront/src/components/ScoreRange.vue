<template>
  <div>
    <h2>Score Range</h2>
    <div v-if="loading">Loading...</div>
    <div v-else-if="error">Error: {{ error }}</div>
    <div v-else>
      <p>Score Range: {{ scoreRange.min }} - {{ scoreRange.max }}</p>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import axios from 'axios';

export default {
  name: 'ScoreRange',
  setup() {
    const scoreRange = ref({min: null, max: null});
    const loading = ref(true);
    const error = ref(null);

    const fetchData = async () => {
      try {
        const response = await axios.get('/api/score/range');
        scoreRange.value = response.data;
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
      scoreRange,
      loading,
      error,
    };
  }
};
</script>
