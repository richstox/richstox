/**
 * Timezone Onboarding Screen
 * ==========================
 * Allows user to select their country/timezone during onboarding.
 * App displays all timestamps in user's local timezone.
 */

import React, { useState } from 'react';
import { 
  View, 
  Text, 
  TouchableOpacity, 
  StyleSheet, 
  ScrollView,
  TextInput,
} from 'react-native';
import { useRouter } from 'expo-router';
import { useAuth } from '../../contexts/AuthContext';
import { COLORS } from '../_layout';
import { Ionicons } from '@expo/vector-icons';

// Common timezones with countries
const TIMEZONES = [
  { country: 'Czech Republic', timezone: 'Europe/Prague', flag: '🇨🇿' },
  { country: 'Slovakia', timezone: 'Europe/Bratislava', flag: '🇸🇰' },
  { country: 'Poland', timezone: 'Europe/Warsaw', flag: '🇵🇱' },
  { country: 'Germany', timezone: 'Europe/Berlin', flag: '🇩🇪' },
  { country: 'Austria', timezone: 'Europe/Vienna', flag: '🇦🇹' },
  { country: 'United Kingdom', timezone: 'Europe/London', flag: '🇬🇧' },
  { country: 'France', timezone: 'Europe/Paris', flag: '🇫🇷' },
  { country: 'Spain', timezone: 'Europe/Madrid', flag: '🇪🇸' },
  { country: 'Italy', timezone: 'Europe/Rome', flag: '🇮🇹' },
  { country: 'Netherlands', timezone: 'Europe/Amsterdam', flag: '🇳🇱' },
  { country: 'Belgium', timezone: 'Europe/Brussels', flag: '🇧🇪' },
  { country: 'Switzerland', timezone: 'Europe/Zurich', flag: '🇨🇭' },
  { country: 'United States (Eastern)', timezone: 'America/New_York', flag: '🇺🇸' },
  { country: 'United States (Central)', timezone: 'America/Chicago', flag: '🇺🇸' },
  { country: 'United States (Mountain)', timezone: 'America/Denver', flag: '🇺🇸' },
  { country: 'United States (Pacific)', timezone: 'America/Los_Angeles', flag: '🇺🇸' },
  { country: 'Canada (Eastern)', timezone: 'America/Toronto', flag: '🇨🇦' },
  { country: 'Australia (Sydney)', timezone: 'Australia/Sydney', flag: '🇦🇺' },
  { country: 'Japan', timezone: 'Asia/Tokyo', flag: '🇯🇵' },
  { country: 'Singapore', timezone: 'Asia/Singapore', flag: '🇸🇬' },
  { country: 'Hong Kong', timezone: 'Asia/Hong_Kong', flag: '🇭🇰' },
];

export default function TimezoneOnboarding() {
  const router = useRouter();
  const { updateTimezone, user } = useAuth();
  const [selectedTimezone, setSelectedTimezone] = useState<string | null>(null);
  const [selectedCountry, setSelectedCountry] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const filteredTimezones = TIMEZONES.filter(tz => 
    tz.country.toLowerCase().includes(searchQuery.toLowerCase()) ||
    tz.timezone.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const handleSelect = (tz: typeof TIMEZONES[0]) => {
    setSelectedTimezone(tz.timezone);
    setSelectedCountry(tz.country);
  };

  const handleContinue = async () => {
    if (!selectedTimezone || !selectedCountry) return;
    
    setIsLoading(true);
    try {
      await updateTimezone(selectedTimezone, selectedCountry);
      router.replace('/(tabs)/dashboard');
    } catch (error) {
      console.error('Error updating timezone:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSkip = () => {
    router.replace('/(tabs)/dashboard');
  };

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <Text style={styles.title}>Select your country</Text>
        <Text style={styles.subtitle}>
          Times in the app will be shown in your local timezone
        </Text>
      </View>

      {/* Search */}
      <View style={styles.searchContainer}>
        <Ionicons name="search" size={20} color={COLORS.textMuted} />
        <TextInput
          style={styles.searchInput}
          placeholder="Search country..."
          placeholderTextColor={COLORS.textMuted}
          value={searchQuery}
          onChangeText={setSearchQuery}
        />
      </View>

      {/* Timezone List */}
      <ScrollView style={styles.list} showsVerticalScrollIndicator={false}>
        {filteredTimezones.map((tz) => (
          <TouchableOpacity
            key={tz.timezone}
            style={[
              styles.timezoneItem,
              selectedTimezone === tz.timezone && styles.timezoneItemSelected,
            ]}
            onPress={() => handleSelect(tz)}
            data-testid={`timezone-${tz.timezone}`}
          >
            <Text style={styles.flag}>{tz.flag}</Text>
            <View style={styles.timezoneInfo}>
              <Text style={styles.countryName}>{tz.country}</Text>
              <Text style={styles.timezoneName}>{tz.timezone}</Text>
            </View>
            {selectedTimezone === tz.timezone && (
              <Ionicons name="checkmark-circle" size={24} color={COLORS.primary} />
            )}
          </TouchableOpacity>
        ))}
      </ScrollView>

      {/* Continue Button */}
      <View style={styles.footer}>
        <TouchableOpacity
          style={[
            styles.continueButton,
            !selectedTimezone && styles.continueButtonDisabled,
          ]}
          onPress={handleContinue}
          disabled={!selectedTimezone || isLoading}
          data-testid="continue-btn"
        >
          <Text style={styles.continueButtonText}>
            {isLoading ? 'Saving...' : 'Continue'}
          </Text>
        </TouchableOpacity>

        <TouchableOpacity
          style={styles.skipButton}
          onPress={handleSkip}
          data-testid="skip-timezone-btn"
        >
          <Text style={styles.skipText}>Skip</Text>
        </TouchableOpacity>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
    paddingHorizontal: 20,
    paddingTop: 60,
  },
  header: {
    marginBottom: 24,
  },
  title: {
    fontSize: 28,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    color: COLORS.textLight,
    lineHeight: 22,
  },
  searchContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.surface,
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  searchInput: {
    flex: 1,
    fontSize: 16,
    color: COLORS.text,
    marginLeft: 12,
  },
  list: {
    flex: 1,
  },
  timezoneItem: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.surface,
    borderRadius: 12,
    padding: 16,
    marginBottom: 8,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  timezoneItemSelected: {
    borderColor: COLORS.primary,
    backgroundColor: '#F0F4F8',
  },
  flag: {
    fontSize: 28,
    marginRight: 12,
  },
  timezoneInfo: {
    flex: 1,
  },
  countryName: {
    fontSize: 16,
    fontWeight: '500',
    color: COLORS.text,
  },
  timezoneName: {
    fontSize: 13,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  footer: {
    paddingVertical: 20,
  },
  continueButton: {
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    alignItems: 'center',
  },
  continueButtonDisabled: {
    opacity: 0.5,
  },
  continueButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  skipButton: {
    marginTop: 12,
    alignItems: 'center',
    paddingVertical: 12,
  },
  skipText: {
    fontSize: 14,
    color: COLORS.textMuted,
  },
});
