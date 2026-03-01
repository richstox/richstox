import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
  Alert,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from '../_layout';
import { useAuth } from '../../contexts/AuthContext';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;

export default function Settings() {
  const router = useRouter();
  const { isAdmin } = useAuth();
  const [portfolioName, setPortfolioName] = useState('');

  useEffect(() => {
    loadPortfolioInfo();
  }, []);

  const loadPortfolioInfo = async () => {
    try {
      const portfolioId = await AsyncStorage.getItem('portfolioId');
      if (portfolioId) {
        const response = await axios.get(`${API_URL}/api/portfolios/${portfolioId}`);
        setPortfolioName(response.data.name);
      }
    } catch (error) {
      console.error('Error loading portfolio info:', error);
    }
  };

  const handleResetApp = () => {
    Alert.alert(
      'Reset App',
      'This will delete all your portfolio data and start fresh. This action cannot be undone.',
      [
        { text: 'Cancel', style: 'cancel' },
        {
          text: 'Reset',
          style: 'destructive',
          onPress: async () => {
            try {
              const portfolioId = await AsyncStorage.getItem('portfolioId');
              if (portfolioId) {
                await axios.delete(`${API_URL}/api/portfolios/${portfolioId}`);
              }
              await AsyncStorage.clear();
              router.replace('/onboarding/step1');
            } catch (error) {
              console.error('Error resetting app:', error);
              await AsyncStorage.clear();
              router.replace('/onboarding/step1');
            }
          },
        },
      ]
    );
  };

  return (
    <SafeAreaView style={styles.container} edges={['left', 'right']}>
      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={styles.scrollContent}
        showsVerticalScrollIndicator={false}
      >
        {/* App Info */}
        <View style={styles.appHeader}>
          <Text style={styles.appName}>RICHSTOX</Text>
          <Text style={styles.appTagline}>Calm investing, lasting wealth</Text>
          <Text style={styles.appVersion}>Version 1.0.0</Text>
        </View>

        {/* Portfolio Section */}
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Portfolio</Text>
          <View style={styles.sectionCard}>
            <View style={styles.settingItem}>
              <View style={styles.settingLeft}>
                <Ionicons name="briefcase-outline" size={22} color={COLORS.primary} />
                <View style={styles.settingText}>
                  <Text style={styles.settingLabel}>Current Portfolio</Text>
                  <Text style={styles.settingValue}>{portfolioName || 'Not set'}</Text>
                </View>
              </View>
            </View>
          </View>
        </View>

        {/* Admin Section - only shown for admins */}
        {isAdmin && (
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Administration</Text>
            <View style={styles.sectionCard}>
              <TouchableOpacity 
                style={styles.settingItem} 
                onPress={() => router.push('/admin')}
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

        {/* Legal Section */}
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Legal & Disclaimer</Text>
          <View style={styles.sectionCard}>
            <View style={styles.disclaimerBox}>
              <Ionicons name="information-circle-outline" size={24} color={COLORS.warning} />
              <Text style={styles.disclaimerTitle}>Educational Purpose Only</Text>
            </View>
            <Text style={styles.disclaimerText}>
              RICHSTOX is an educational tool designed to help you track and understand your investment portfolio. This app:
            </Text>
            <View style={styles.disclaimerPoints}>
              <View style={styles.disclaimerPoint}>
                <Ionicons name="close-circle" size={16} color={COLORS.negative} />
                <Text style={styles.disclaimerPointText}>Does NOT execute trades</Text>
              </View>
              <View style={styles.disclaimerPoint}>
                <Ionicons name="close-circle" size={16} color={COLORS.negative} />
                <Text style={styles.disclaimerPointText}>Does NOT provide financial advice</Text>
              </View>
              <View style={styles.disclaimerPoint}>
                <Ionicons name="close-circle" size={16} color={COLORS.negative} />
                <Text style={styles.disclaimerPointText}>Does NOT guarantee any returns</Text>
              </View>
              <View style={styles.disclaimerPoint}>
                <Ionicons name="close-circle" size={16} color={COLORS.negative} />
                <Text style={styles.disclaimerPointText}>Does NOT provide trading signals</Text>
              </View>
            </View>
            <Text style={styles.disclaimerText}>
              All investment decisions are solely your responsibility. Past performance does not guarantee future results. Always consult a qualified financial advisor before making investment decisions.
            </Text>
          </View>
        </View>

        {/* About Section */}
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

        {/* Danger Zone */}
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
                <Text style={styles.dangerLabel}>Reset App & Delete Data</Text>
              </View>
            </TouchableOpacity>
          </View>
        </View>

        {/* Footer */}
        <View style={styles.footer}>
          <Text style={styles.footerText}>
            Made with patience for long-term investors
          </Text>
          <Text style={styles.footerQuote}>
            "Nothing to do today is success."
          </Text>
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
    padding: 16,
    paddingBottom: 48,
  },
  appHeader: {
    alignItems: 'center',
    paddingVertical: 24,
    marginBottom: 8,
  },
  appName: {
    fontSize: 28,
    fontWeight: '700',
    color: COLORS.text,
    letterSpacing: 3,
  },
  appTagline: {
    fontSize: 14,
    color: COLORS.textLight,
    marginTop: 4,
    fontStyle: 'italic',
  },
  appVersion: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 8,
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
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: 16,
  },
  settingLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  settingText: {},
  settingLabel: {
    fontSize: 15,
    color: COLORS.text,
  },
  settingValue: {
    fontSize: 13,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  divider: {
    height: 1,
    backgroundColor: COLORS.border,
    marginLeft: 50,
  },
  disclaimerBox: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
    padding: 16,
    paddingBottom: 8,
  },
  disclaimerTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  disclaimerText: {
    fontSize: 14,
    color: COLORS.textLight,
    lineHeight: 20,
    paddingHorizontal: 16,
    paddingVertical: 8,
  },
  disclaimerPoints: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    gap: 8,
  },
  disclaimerPoint: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  disclaimerPointText: {
    fontSize: 14,
    color: COLORS.textLight,
  },
  dangerItem: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: 16,
  },
  dangerLabel: {
    fontSize: 15,
    color: COLORS.negative,
  },
  footer: {
    alignItems: 'center',
    paddingVertical: 24,
  },
  footerText: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  footerQuote: {
    fontSize: 14,
    color: COLORS.textLight,
    fontStyle: 'italic',
    marginTop: 8,
  },
});
