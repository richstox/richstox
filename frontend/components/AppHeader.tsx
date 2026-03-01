/**
 * AppHeader - Global header component for the entire RICHSTOX app
 * DO NOT MODIFY without Richard's approval.
 * 
 * Features:
 * - Avatar dropdown menu with: My Dashboard, Account Settings, Admin Panel (admin only), Sign out
 * - Consistent across all pages including /admin
 */

import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  Image,
  Platform,
  Pressable,
} from 'react-native';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { useAuth } from '../contexts/AuthContext';

const COLORS = {
  primary: '#1E3A5F',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
  danger: '#EF4444',
};

interface AppHeaderProps {
  showBackButton?: boolean;
  backDestination?: string;
  onNotificationPress?: () => void;
  notificationCount?: number;
}

export default function AppHeader({ 
  showBackButton = false, 
  backDestination,
  onNotificationPress,
  notificationCount = 0,
}: AppHeaderProps) {
  const router = useRouter();
  const { user, isAdmin, logout } = useAuth();
  const [showMenu, setShowMenu] = useState(false);

  const handleBack = () => {
    if (backDestination) {
      router.push(backDestination as any);
    } else {
      router.back();
    }
  };

  const handleMenuItemPress = (action: string) => {
    setShowMenu(false);
    switch (action) {
      case 'dashboard':
        router.push('/(tabs)/dashboard');
        break;
      case 'settings':
        router.push('/(tabs)/settings');
        break;
      case 'admin':
        router.push('/admin');
        break;
      case 'signout':
        logout();
        break;
    }
  };

  return (
    <View style={styles.header} testID="app-header">
      {/* Left side: Back button or Logo */}
      {showBackButton ? (
        <TouchableOpacity 
          style={styles.backButton} 
          onPress={handleBack}
          testID="header-back-btn"
        >
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
      ) : (
        <Image 
          source={require('../assets/images/richstox_icon.png')} 
          style={styles.logo} 
        />
      )}
      
      <Text style={styles.headerTitle}>RICHSTOX</Text>
      
      {/* Right side: Search, Notifications, PRO badge, Avatar */}
      <View style={styles.headerRight}>
        <TouchableOpacity 
          style={styles.headerIcon} 
          onPress={() => router.push('/(tabs)/search')}
          testID="header-search-btn"
        >
          <Ionicons name="search-outline" size={22} color={COLORS.text} />
        </TouchableOpacity>
        
        {onNotificationPress && (
          <TouchableOpacity 
            style={styles.headerIcon} 
            onPress={onNotificationPress}
            testID="header-notifications-btn"
          >
            <Ionicons 
              name={notificationCount > 0 ? "notifications" : "notifications-outline"} 
              size={22} 
              color={notificationCount > 0 ? COLORS.danger : COLORS.text} 
            />
            {notificationCount > 0 && (
              <View style={styles.notificationBadge}>
                <Text style={styles.notificationBadgeText}>
                  {notificationCount > 9 ? '9+' : notificationCount}
                </Text>
              </View>
            )}
          </TouchableOpacity>
        )}
        
        {/* Subscription Badge */}
        <View style={[
          styles.subscriptionBadge,
          user?.subscription_tier === 'pro' && styles.subscriptionBadgePro,
          user?.subscription_tier === 'pro_plus' && styles.subscriptionBadgeProPlus,
        ]}>
          {(user?.subscription_tier === 'pro' || user?.subscription_tier === 'pro_plus') && (
            <Ionicons name="sparkles" size={10} color="#FFF" style={{ marginRight: 3 }} />
          )}
          <Text style={[
            styles.subscriptionBadgeText,
            (user?.subscription_tier === 'pro' || user?.subscription_tier === 'pro_plus') && styles.subscriptionBadgeTextPro,
          ]}>
            {user?.subscription_tier === 'pro_plus' ? 'PRO+' : 
             user?.subscription_tier === 'pro' ? 'PRO' : 'FREE'}
          </Text>
        </View>
        
        {/* Avatar with dropdown menu */}
        <View style={styles.avatarContainer}>
          <TouchableOpacity 
            style={styles.avatar} 
            onPress={() => setShowMenu(!showMenu)}
            testID="header-avatar-btn"
          >
            {user?.picture ? (
              <Image source={{ uri: user.picture }} style={styles.avatarImage} />
            ) : (
              <Ionicons name="person" size={18} color={COLORS.textMuted} />
            )}
          </TouchableOpacity>

          {/* Dropdown Menu - positioned absolutely below avatar */}
          {showMenu && (
            <>
              {/* Invisible overlay to catch clicks outside menu */}
              <Pressable 
                style={styles.menuOverlay}
                onPress={() => setShowMenu(false)}
              />
              <View style={styles.menuContainer} testID="avatar-menu">
                {/* User info */}
                <View style={styles.menuUserInfo}>
                  {user?.picture ? (
                    <Image source={{ uri: user.picture }} style={styles.menuAvatar} />
                  ) : (
                    <View style={styles.menuAvatarPlaceholder}>
                      <Ionicons name="person" size={24} color={COLORS.textMuted} />
                    </View>
                  )}
                  <View style={styles.menuUserText}>
                    <Text style={styles.menuUserName}>{user?.name || 'User'}</Text>
                    <Text style={styles.menuUserEmail}>{user?.email}</Text>
                  </View>
                </View>
                
                <View style={styles.menuDivider} />
                
                {/* Menu Items */}
                <TouchableOpacity 
                  style={styles.menuItem}
                  onPress={() => handleMenuItemPress('dashboard')}
                  testID="menu-my-dashboard"
                >
                  <Ionicons name="home-outline" size={20} color={COLORS.text} />
                  <Text style={styles.menuItemText}>My Dashboard</Text>
                </TouchableOpacity>
                
                <TouchableOpacity 
                  style={styles.menuItem}
                  onPress={() => handleMenuItemPress('settings')}
                  testID="menu-account-settings"
                >
                  <Ionicons name="settings-outline" size={20} color={COLORS.text} />
                  <Text style={styles.menuItemText}>Account Settings</Text>
                </TouchableOpacity>
                
                {/* Admin Panel - only for admins */}
                {isAdmin && (
                  <TouchableOpacity 
                    style={styles.menuItem}
                    onPress={() => handleMenuItemPress('admin')}
                    testID="menu-admin-panel"
                  >
                    <Ionicons name="shield-outline" size={20} color={COLORS.primary} />
                    <Text style={[styles.menuItemText, { color: COLORS.primary }]}>Admin Panel</Text>
                  </TouchableOpacity>
                )}
                
                <View style={styles.menuDivider} />
                
                <TouchableOpacity 
                  style={styles.menuItem}
                  onPress={() => handleMenuItemPress('signout')}
                  testID="menu-sign-out"
                >
                  <Ionicons name="log-out-outline" size={20} color={COLORS.danger} />
                  <Text style={[styles.menuItemText, { color: COLORS.danger }]}>Sign out</Text>
                </TouchableOpacity>
              </View>
            </>
          )}
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    zIndex: 1000,
  },
  backButton: {
    padding: 4,
    marginRight: 8,
  },
  logo: { 
    width: 36, 
    height: 36 
  },
  headerTitle: {
    flex: 1,
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.primary,
    marginLeft: 10,
    letterSpacing: 1,
  },
  headerRight: { 
    flexDirection: 'row', 
    alignItems: 'center', 
    gap: 12 
  },
  headerIcon: { 
    padding: 4, 
    position: 'relative' 
  },
  notificationBadge: {
    position: 'absolute',
    top: 0,
    right: 0,
    backgroundColor: COLORS.danger,
    borderRadius: 10,
    minWidth: 18,
    height: 18,
    justifyContent: 'center',
    alignItems: 'center',
    paddingHorizontal: 4,
  },
  notificationBadgeText: {
    color: '#FFFFFF',
    fontSize: 10,
    fontWeight: '700',
  },
  subscriptionBadge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 10,
    backgroundColor: COLORS.border,
    marginLeft: 6,
    flexDirection: 'row',
    alignItems: 'center',
  },
  subscriptionBadgePro: {
    backgroundColor: '#8B5CF6',
  },
  subscriptionBadgeProPlus: {
    backgroundColor: '#6D28D9',
  },
  subscriptionBadgeText: {
    fontSize: 10,
    fontWeight: '700',
    color: COLORS.textMuted,
  },
  subscriptionBadgeTextPro: {
    color: '#FFFFFF',
  },
  avatarContainer: {
    position: 'relative',
    zIndex: 1001,
  },
  avatar: {
    width: 36,
    height: 36,
    borderRadius: 18,
    backgroundColor: COLORS.background,
    alignItems: 'center',
    justifyContent: 'center',
    borderWidth: 2,
    borderColor: COLORS.border,
    cursor: 'pointer',
  },
  avatarImage: { 
    width: 32, 
    height: 32, 
    borderRadius: 16 
  },
  
  // Dropdown Menu Styles
  menuOverlay: {
    position: 'fixed' as any,
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: 'transparent',
    zIndex: 999,
  },
  menuContainer: {
    position: 'absolute',
    top: 44,
    right: 0,
    backgroundColor: COLORS.card,
    borderRadius: 12,
    width: 260,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.15,
    shadowRadius: 12,
    elevation: 8,
    overflow: 'hidden',
    zIndex: 1002,
    ...(Platform.OS === 'web' ? {
      boxShadow: '0 4px 20px rgba(0, 0, 0, 0.15)',
    } : {}),
  },
  menuUserInfo: {
    flexDirection: 'row',
    alignItems: 'center',
    padding: 16,
    gap: 12,
  },
  menuAvatar: {
    width: 44,
    height: 44,
    borderRadius: 22,
  },
  menuAvatarPlaceholder: {
    width: 44,
    height: 44,
    borderRadius: 22,
    backgroundColor: COLORS.background,
    alignItems: 'center',
    justifyContent: 'center',
  },
  menuUserText: {
    flex: 1,
  },
  menuUserName: {
    fontSize: 15,
    fontWeight: '600',
    color: COLORS.text,
  },
  menuUserEmail: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  menuDivider: {
    height: 1,
    backgroundColor: COLORS.border,
  },
  menuItem: {
    flexDirection: 'row',
    alignItems: 'center',
    padding: 14,
    gap: 12,
    cursor: 'pointer',
  },
  menuItemText: {
    fontSize: 15,
    color: COLORS.text,
  },
});
