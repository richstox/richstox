import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  SafeAreaView,
  TextInput,
  KeyboardAvoidingView,
  Platform,
  ScrollView,
} from 'react-native';
import { useRouter } from 'expo-router';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';

export default function OnboardingStep2() {
  const router = useRouter();
  const [portfolioName, setPortfolioName] = useState('');
  const [portfolioType, setPortfolioType] = useState<string | null>(null);

  const portfolioTypes = [
    { id: 'growth', label: 'Growth', icon: 'trending-up-outline' },
    { id: 'dividend', label: 'Dividend', icon: 'cash-outline' },
    { id: 'balanced', label: 'Balanced', icon: 'scale-outline' },
    { id: 'value', label: 'Value', icon: 'diamond-outline' },
  ];

  const handleContinue = async () => {
    if (portfolioName.trim() && portfolioType) {
      await AsyncStorage.setItem('onboardingPortfolioName', portfolioName.trim());
      await AsyncStorage.setItem('onboardingPortfolioType', portfolioType);
      router.push('/onboarding/step3');
    }
  };

  const canContinue = portfolioName.trim().length > 0 && portfolioType;

  return (
    <SafeAreaView style={styles.container}>
      <KeyboardAvoidingView
        behavior={Platform.OS === 'ios' ? 'padding' : 'height'}
        style={styles.keyboardView}
      >
        <ScrollView 
          style={styles.scrollView}
          contentContainerStyle={styles.scrollContent}
          keyboardShouldPersistTaps="handled"
        >
          <TouchableOpacity
            style={styles.backButton}
            onPress={() => router.back()}
          >
            <Ionicons name="arrow-back" size={24} color={COLORS.text} />
          </TouchableOpacity>

          <View style={styles.header}>
            <Text style={styles.stepText}>Step 2 of 3</Text>
            <Text style={styles.title}>Name your portfolio</Text>
            <Text style={styles.subtitle}>
              Give it a meaningful name and choose a strategy focus
            </Text>
          </View>

          <View style={styles.inputContainer}>
            <Text style={styles.label}>Portfolio Name</Text>
            <TextInput
              style={styles.textInput}
              placeholder="e.g., Long-Term Growth"
              placeholderTextColor={COLORS.textMuted}
              value={portfolioName}
              onChangeText={setPortfolioName}
              maxLength={30}
            />
          </View>

          <View style={styles.typeContainer}>
            <Text style={styles.label}>Strategy Focus</Text>
            <View style={styles.typeGrid}>
              {portfolioTypes.map((type) => (
                <TouchableOpacity
                  key={type.id}
                  style={[
                    styles.typeCard,
                    portfolioType === type.id && styles.typeCardSelected,
                  ]}
                  onPress={() => setPortfolioType(type.id)}
                  activeOpacity={0.7}
                >
                  <Ionicons
                    name={type.icon as any}
                    size={24}
                    color={portfolioType === type.id ? COLORS.primary : COLORS.textLight}
                  />
                  <Text
                    style={[
                      styles.typeLabel,
                      portfolioType === type.id && styles.typeLabelSelected,
                    ]}
                  >
                    {type.label}
                  </Text>
                </TouchableOpacity>
              ))}
            </View>
          </View>

          <TouchableOpacity
            style={[
              styles.continueButton,
              !canContinue && styles.continueButtonDisabled,
            ]}
            onPress={handleContinue}
            disabled={!canContinue}
            activeOpacity={0.8}
          >
            <Text style={styles.continueButtonText}>Continue</Text>
            <Ionicons name="arrow-forward" size={20} color="#FFFFFF" />
          </TouchableOpacity>
        </ScrollView>
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  keyboardView: {
    flex: 1,
  },
  scrollView: {
    flex: 1,
  },
  scrollContent: {
    paddingHorizontal: 24,
    paddingTop: 16,
    paddingBottom: 40,
  },
  backButton: {
    width: 44,
    height: 44,
    alignItems: 'center',
    justifyContent: 'center',
    marginLeft: -8,
  },
  header: {
    marginBottom: 32,
    marginTop: 8,
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
  inputContainer: {
    marginBottom: 32,
  },
  label: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 12,
  },
  textInput: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 16,
    color: COLORS.text,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  typeContainer: {
    flex: 1,
    marginBottom: 32,
  },
  typeGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 12,
  },
  typeCard: {
    width: '47%',
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 20,
    alignItems: 'center',
    borderWidth: 2,
    borderColor: COLORS.border,
  },
  typeCardSelected: {
    borderColor: COLORS.primary,
    backgroundColor: '#F5F8FC',
  },
  typeLabel: {
    fontSize: 14,
    fontWeight: '500',
    color: COLORS.textLight,
    marginTop: 8,
  },
  typeLabelSelected: {
    color: COLORS.primary,
  },
  continueButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    gap: 8,
    marginTop: 'auto',
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
