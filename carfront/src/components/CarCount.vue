<template>
  <div>
    <h2>Car Count</h2>
    <div v-if="loading">Loading...</div>
    <div v-else-if="error">Error: {{ error }}</div>
    <div v-else>
      <ul>
        <li v-for="car in cars" :key="car.id">{{ car.model }}: {{ car.count }}</li>
      </ul>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import axios from 'axios';

export default {
  name: 'CarCount',
  setup() {
    const cars = ref([]);
    const loading = ref(true);
    const error = ref(null);

    const fetchData = async () => {
      try {
        const response = await axios.get('/api/car/count');
        cars.value = response.data;
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
      cars,
      loading,
      error,
    };
  }
};
</script>
