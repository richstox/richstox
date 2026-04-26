/**
 * AppHeader - Global header component for the entire RICHSTOX app
 * DO NOT MODIFY without Richard's approval.
 * 
 * Features:
 * - Avatar dropdown menu with: Portfolio, Account Settings, Admin Panel (admin only), Sign out
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
  ActivityIndicator,
} from 'react-native';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { useAuth } from '../contexts/AuthContext';
import { FONTS } from '../app/_layout';
import { useLayoutSpacing } from '../constants/layout';

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
  title?: string;
  showBackButton?: boolean;
  backDestination?: string;
  onNotificationPress?: () => void;
  notificationCount?: number;
  showSubscriptionBadge?: boolean;
  rightAction?: React.ReactNode;
}

export default function AppHeader({ 
  title = 'RICHSTOX',
  showBackButton = false, 
  backDestination,
  onNotificationPress,
  notificationCount = 0,
  showSubscriptionBadge = true,
  rightAction,
}: AppHeaderProps) {
  const router = useRouter();
  const { user, isAdmin, logout, isSessionValidated } = useAuth();
  const [showMenu, setShowMenu] = useState(false);
  const sp = useLayoutSpacing();

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
      case 'portfolio':
        router.push('/(tabs)/portfolio');
        break;
      case 'settings':
        router.push('/(tabs)/settings');
        break;
      case 'admin':
        router.push('/(tabs)/admin' as any);
        break;
      case 'signout':
        logout();
        break;
    }
  };

  return (
    <View style={[styles.header, { paddingHorizontal: sp.pageGutter }]} testID="app-header">
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
        <TouchableOpacity onPress={() => router.push('/(tabs)/dashboard')} testID="header-logo-btn">
          <Image 
            source={require('../assets/images/richstox_icon.png')} 
            style={styles.logo} 
          />
        </TouchableOpacity>
      )}
      
      <Text style={styles.headerTitle}>{title}</Text>
      
      {/* Right side: Search, Notifications, PRO badge, Avatar */}
      <View style={[styles.headerRight, { gap: sp.rowGap }]}>
        {rightAction}
        {onNotificationPress ? (
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
        ) : (
          <TouchableOpacity 
            style={styles.headerIcon} 
            onPress={() => router.push('/(tabs)/dashboard')}
            testID="header-notifications-btn"
          >
            <Ionicons name="notifications-outline" size={22} color={COLORS.text} />
          </TouchableOpacity>
        )}
        
        {/* Subscription Badge - only show for PRO/PRO+ users */}
        {showSubscriptionBadge && (user?.subscription_tier === 'pro' || user?.subscription_tier === 'pro_plus') && (
        <View style={[
          styles.subscriptionBadge,
          user?.subscription_tier === 'pro' && styles.subscriptionBadgePro,
          user?.subscription_tier === 'pro_plus' && styles.subscriptionBadgeProPlus,
        ]}>
          <Ionicons name="sparkles" size={10} color="#FFF" style={{ marginRight: 3 }} />
          <Text style={[
            styles.subscriptionBadgeText,
            styles.subscriptionBadgeTextPro,
          ]}>
            {user?.subscription_tier === 'pro_plus' ? 'PRO+' : 'PRO'}
          </Text>
        </View>
        )}
        
        {/* Avatar with dropdown menu - only interactive when user is fully loaded */}
        <View style={styles.avatarContainer}>
          {!isSessionValidated ? (
            /* Session validation in progress — show inert placeholder */
            <View style={styles.avatar} testID="header-avatar-loading">
              <ActivityIndicator size="small" color={COLORS.textMuted} />
            </View>
          ) : !user ? (
            /* No authenticated user — show inert placeholder (page will redirect) */
            <View style={styles.avatar} testID="header-avatar-placeholder">
              <Ionicons name="person" size={18} color={COLORS.textMuted} />
            </View>
          ) : (
            <>
              <TouchableOpacity 
                style={styles.avatar} 
                onPress={() => setShowMenu(!showMenu)}
                testID="header-avatar-btn"
              >
                {user.picture ? (
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
                      {user.picture ? (
                        <Image source={{ uri: user.picture }} style={styles.menuAvatar} />
                      ) : (
                        <View style={styles.menuAvatarPlaceholder}>
                          <Ionicons name="person" size={24} color={COLORS.textMuted} />
                        </View>
                      )}
                      <View style={styles.menuUserText}>
                        <Text style={styles.menuUserName}>{user.name || 'User'}</Text>
                        <Text style={styles.menuUserEmail}>{user.email}</Text>
                        {/* Subscription badge inside menu */}
                        <View style={[
                          styles.menuSubscriptionBadge,
                          (user.subscription_tier === 'pro') && styles.subscriptionBadgePro,
                          (user.subscription_tier === 'pro_plus') && styles.subscriptionBadgeProPlus,
                        ]}>
                          {(user.subscription_tier === 'pro' || user.subscription_tier === 'pro_plus') && (
                            <Ionicons name="sparkles" size={10} color="#FFF" style={{ marginRight: 3 }} />
                          )}
                          <Text style={[
                            styles.subscriptionBadgeText,
                            (user.subscription_tier === 'pro' || user.subscription_tier === 'pro_plus') && styles.subscriptionBadgeTextPro,
                          ]}>
                            {user.subscription_tier === 'pro_plus' ? 'PRO+' : 
                             user.subscription_tier === 'pro' ? 'PRO' : 'FREE'}
                          </Text>
                        </View>
                      </View>
                    </View>
                    
                    <View style={styles.menuDivider} />
                    
                    {/* Menu Items */}
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

                    <View
                      style={[styles.menuItem, styles.menuItemDisabled]}
                      testID="menu-portfolio"
                    >
                      <Ionicons name="briefcase-outline" size={20} color={COLORS.textMuted} />
                      <Text style={[styles.menuItemText, styles.menuItemTextDisabled]}>Portfolio</Text>
                      <View style={styles.menuSoonBadge}>
                        <Text style={styles.menuSoonBadgeText}>Soon</Text>
                      </View>
                    </View>
                    
                    <TouchableOpacity 
                      style={styles.menuItem}
                      onPress={() => handleMenuItemPress('settings')}
                      testID="menu-account-settings"
                    >
                      <Ionicons name="settings-outline" size={20} color={COLORS.text} />
                      <Text style={styles.menuItemText}>Account Settings</Text>
                    </TouchableOpacity>
                    
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
    fontFamily: FONTS.heading,
    color: COLORS.primary,
    marginLeft: 10,
    letterSpacing: 0,
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
    fontFamily: FONTS.bodySemiBold,
    color: COLORS.text,
  },
  menuUserEmail: {
    fontSize: 12,
    fontFamily: FONTS.body,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  menuSubscriptionBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    alignSelf: 'flex-start',
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 10,
    backgroundColor: COLORS.border,
    marginTop: 6,
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
    fontFamily: FONTS.body,
    color: COLORS.text,
  },
  menuItemDisabled: {
    cursor: 'default',
    opacity: 0.6,
  },
  menuItemTextDisabled: {
    color: COLORS.textMuted,
  },
  menuSoonBadge: {
    marginLeft: 'auto',
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 999,
    backgroundColor: '#FEF3C7',
  },
  menuSoonBadgeText: {
    fontSize: 11,
    fontFamily: FONTS.bodyMedium,
    color: '#B45309',
  },
});
