import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  SafeAreaView,
} from 'react-native';
import { useRouter } from 'expo-router';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';

export default function OnboardingStep1() {
  const router = useRouter();
  const [selectedGoal, setSelectedGoal] = useState<string | null>(null);

  const handleContinue = async () => {
    if (selectedGoal) {
      await AsyncStorage.setItem('onboardingGoal', selectedGoal);
      router.push('/onboarding/step2');
    }
  };

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.content}>
        <View style={styles.header}>
          <Text style={styles.stepText}>Step 1 of 3</Text>
          <Text style={styles.title}>What's your goal?</Text>
          <Text style={styles.subtitle}>
            Choose how you want to use RICHSTOX
          </Text>
        </View>

        <View style={styles.optionsContainer}>
          <TouchableOpacity
            style={[
              styles.optionCard,
              selectedGoal === 'track' && styles.optionCardSelected,
            ]}
            onPress={() => setSelectedGoal('track')}
            activeOpacity={0.7}
          >
            <View style={styles.optionIcon}>
              <Ionicons
                name="analytics-outline"
                size={32}
                color={selectedGoal === 'track' ? COLORS.primary : COLORS.textLight}
              />
            </View>
            <Text
              style={[
                styles.optionTitle,
                selectedGoal === 'track' && styles.optionTitleSelected,
              ]}
            >
              Track Real Portfolio
            </Text>
            <Text style={styles.optionDescription}>
              Monitor your actual investments with discipline and clarity
            </Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[
              styles.optionCard,
              selectedGoal === 'simulate' && styles.optionCardSelected,
            ]}
            onPress={() => setSelectedGoal('simulate')}
            activeOpacity={0.7}
          >
            <View style={styles.optionIcon}>
              <Ionicons
                name="flask-outline"
                size={32}
                color={selectedGoal === 'simulate' ? COLORS.primary : COLORS.textLight}
              />
            </View>
            <Text
              style={[
                styles.optionTitle,
                selectedGoal === 'simulate' && styles.optionTitleSelected,
              ]}
            >
              Simulate & Learn
            </Text>
            <Text style={styles.optionDescription}>
              Practice investing strategies without real money
            </Text>
          </TouchableOpacity>
        </View>

        <TouchableOpacity
          style={[
            styles.continueButton,
            !selectedGoal && styles.continueButtonDisabled,
          ]}
          onPress={handleContinue}
          disabled={!selectedGoal}
          activeOpacity={0.8}
        >
          <Text style={styles.continueButtonText}>Continue</Text>
          <Ionicons name="arrow-forward" size={20} color="#FFFFFF" />
        </TouchableOpacity>
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  content: {
    flex: 1,
    paddingHorizontal: 24,
    paddingTop: 40,
  },
  header: {
    marginBottom: 40,
  },
  stepText: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 8,
  },
  title: {
    fontSize: 28,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    color: COLORS.textLight,
    lineHeight: 24,
  },
  optionsContainer: {
    flex: 1,
    gap: 16,
  },
  optionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 24,
    borderWidth: 2,
    borderColor: COLORS.border,
  },
  optionCardSelected: {
    borderColor: COLORS.primary,
    backgroundColor: '#F5F8FC',
  },
  optionIcon: {
    width: 56,
    height: 56,
    borderRadius: 28,
    backgroundColor: COLORS.background,
    alignItems: 'center',
    justifyContent: 'center',
    marginBottom: 16,
  },
  optionTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 8,
  },
  optionTitleSelected: {
    color: COLORS.primary,
  },
  optionDescription: {
    fontSize: 14,
    color: COLORS.textLight,
    lineHeight: 20,
  },
  continueButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    marginBottom: 40,
    gap: 8,
  },
  continueButtonDisabled: {
    backgroundColor: COLORS.textMuted,
  },
  continueButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
});
