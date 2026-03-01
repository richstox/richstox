import React, { useEffect } from 'react';
import { View, StyleSheet, Image } from 'react-native';
import { useRouter, usePathname, useSegments } from 'expo-router';
import { COLORS } from './_layout';

export default function Index() {
  const router = useRouter();
  const pathname = usePathname();
  const segments = useSegments();

  useEffect(() => {
    // Only redirect to dashboard if we're actually on the root index page
    // Don't redirect if expo-router is navigating to another route
    const isRootIndex = pathname === '/' || pathname === '';
    
    console.log('Index - pathname:', pathname, 'segments:', segments, 'isRootIndex:', isRootIndex);
    
    if (isRootIndex) {
      const timer = setTimeout(() => {
        router.replace('/(tabs)/dashboard');
      }, 500);
      
      return () => clearTimeout(timer);
    }
  }, [pathname]);

  return (
    <View style={styles.container}>
      <Image 
        source={require('../assets/images/richstox_logo.png')}
        style={styles.logo}
        resizeMode="contain"
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
    alignItems: 'center',
    justifyContent: 'center',
  },
  logo: {
    width: 280,
    height: 100,
  },
});
