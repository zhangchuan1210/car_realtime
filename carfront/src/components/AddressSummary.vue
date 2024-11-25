<template>
  <div>
    <h2>Address Summary</h2>
    <div v-if="loading">Loading...</div>
    <div v-else-if="error">Error: {{ error }}</div>
    <div v-else>
      <ul>
        <li v-for="address in addresses" :key="address.id">{{ address.name }}: {{ address.count }}</li>
      </ul>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from 'vue';
import axios from 'axios';

export default {
  name: 'AddressSummary',
  setup() {
    const addresses = ref([]);
    const loading = ref(true);
    const error = ref(null);

    const fetchData = async () => {
      try {
        const response = await axios.get('/api/address/summary');
        addresses.value = response.data;
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
      addresses,
      loading,
      error,
    };
  }
};
</script>