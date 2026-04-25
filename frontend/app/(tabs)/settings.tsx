import React from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';
import { useAuth } from '../../contexts/AuthContext';
import { useAppDialog } from '../../contexts/AppDialogContext';
import AppHeader from '../../components/AppHeader';
import { useLayoutSpacing } from '../../constants/layout';

export default function Settings() {
  const router = useRouter();
  const { isAdmin } = useAuth();
  const dialog = useAppDialog();
  const sp = useLayoutSpacing();

  const handleResetApp = async () => {
    const confirmed = await dialog.confirm(
      'Reset App',
      'This will clear local app state and restart onboarding. Your Watchlist and Tracklist stay managed from their dedicated flows.',
      { confirmLabel: 'Reset', confirmStyle: 'destructive' },
    );
    if (!confirmed) return;

    try {
      await AsyncStorage.multiRemove([
        'portfolioId',
        'onboardingGoal',
        'onboardingPortfolioName',
        'onboardingPortfolioType',
      ]);
      router.replace('/onboarding/step1');
    } catch (error) {
      console.error('Error resetting app:', error);
      router.replace('/onboarding/step1');
    }
  };

  return (
    <SafeAreaView style={styles.container} edges={['left', 'right']}>
      <AppHeader title="Settings" />
      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        showsVerticalScrollIndicator={false}
      >
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Status</Text>
          <View style={styles.sectionCard}>
            <View style={styles.settingItem}>
              <View style={styles.settingLeft}>
                <Ionicons name="briefcase-outline" size={22} color={COLORS.warning} />
                <View style={styles.settingText}>
                  <Text style={styles.settingLabel}>Portfolio</Text>
                  <Text style={styles.settingValue}>Soon — available from the avatar menu only</Text>
                </View>
              </View>
            </View>
          </View>
        </View>

        {isAdmin && (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Administration</Text>
            <View style={styles.sectionCard}>
              <TouchableOpacity
                style={styles.settingItem}
                onPress={() => router.push('/(tabs)/admin' as any)}
                activeOpacity={0.7}
                data-testid="settings-admin-panel-btn"
              >
                <View style={styles.settingLeft}>
                  <Ionicons name="shield-outline" size={22} color={COLORS.primary} />
                  <Text style={styles.settingLabel}>Admin Panel</Text>
                </View>
                <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
              </TouchableOpacity>
            </View>
          </View>
        )}

        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Legal & Disclaimer</Text>
          <View style={styles.sectionCard}>
            <View style={styles.disclaimerBox}>
              <Ionicons name="information-circle-outline" size={24} color={COLORS.warning} />
              <Text style={styles.disclaimerTitle}>Educational Purpose Only</Text>
            </View>
            <Text style={styles.disclaimerText}>
              RICHSTOX is an educational tool designed to help you track and understand your investment process. It does not execute trades, provide financial advice, or guarantee returns.
            </Text>
          </View>
        </View>

        <View style={styles.section}>
          <Text style={styles.sectionTitle}>About</Text>
          <View style={styles.sectionCard}>
            <TouchableOpacity style={styles.settingItem} activeOpacity={0.7}>
              <View style={styles.settingLeft}>
                <Ionicons name="help-circle-outline" size={22} color={COLORS.textLight} />
                <Text style={styles.settingLabel}>Help & Support</Text>
              </View>
              <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
            </TouchableOpacity>
            <View style={styles.divider} />
            <TouchableOpacity style={styles.settingItem} activeOpacity={0.7}>
              <View style={styles.settingLeft}>
                <Ionicons name="document-text-outline" size={22} color={COLORS.textLight} />
                <Text style={styles.settingLabel}>Terms of Service</Text>
              </View>
              <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
            </TouchableOpacity>
            <View style={styles.divider} />
            <TouchableOpacity style={styles.settingItem} activeOpacity={0.7}>
              <View style={styles.settingLeft}>
                <Ionicons name="shield-checkmark-outline" size={22} color={COLORS.textLight} />
                <Text style={styles.settingLabel}>Privacy Policy</Text>
              </View>
              <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
            </TouchableOpacity>
          </View>
        </View>

        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Data Management</Text>
          <View style={styles.sectionCard}>
            <TouchableOpacity
              style={styles.dangerItem}
              onPress={handleResetApp}
              activeOpacity={0.7}
            >
              <View style={styles.settingLeft}>
                <Ionicons name="trash-outline" size={22} color={COLORS.negative} />
                <Text style={styles.dangerLabel}>Reset App</Text>
              </View>
            </TouchableOpacity>
          </View>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  scrollView: {
    flex: 1,
  },
  scrollContent: {
    paddingBottom: 48,
  },
  section: {
    marginBottom: 24,
  },
  sectionTitle: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.textMuted,
    marginBottom: 12,
    marginLeft: 4,
  },
  sectionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    overflow: 'hidden',
  },
  settingItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 18,
    paddingVertical: 16,
  },
  settingLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    flex: 1,
  },
  settingText: {
    flex: 1,
  },
  settingLabel: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  settingValue: {
    marginTop: 4,
    fontSize: 13,
    color: COLORS.textLight,
  },
  disclaimerBox: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
    paddingHorizontal: 18,
    paddingTop: 18,
  },
  disclaimerTitle: {
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.text,
  },
  disclaimerText: {
    paddingHorizontal: 18,
    paddingTop: 12,
    paddingBottom: 18,
    fontSize: 14,
    lineHeight: 21,
    color: COLORS.textLight,
  },
  divider: {
    height: 1,
    backgroundColor: COLORS.border,
    marginHorizontal: 18,
  },
  dangerItem: {
    paddingHorizontal: 18,
    paddingVertical: 16,
  },
  dangerLabel: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.negative,
  },
});
