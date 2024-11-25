<template>
  <div>
    <h2>Price Range</h2>
    <div v-if="loading">Loading...</div>
    <div v-else-if="error">Error: {{ error }}</div>
    <div v-else>
      <p>Price Range: {{ priceRange.min }} - {{ priceRange.max }} USD</p>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import axios from 'axios';

export default {
  name: 'PriceRange',
  setup() {
    const priceRange = ref({ min: null, max: null });
    const loading = ref(true);
    const error = ref(null);

    const fetchData = async () => {
      try {
        const response = await axios.get('/api/price/range');
        priceRange.value = response.data;
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
      priceRange,
      loading,
      error,
    };
  }
};
</script>