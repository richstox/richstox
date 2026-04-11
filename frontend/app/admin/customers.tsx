/**
 * RICHSTOX Admin Customers
 * User management — view, change tier, suspend, delete
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  TextInput, ActivityIndicator, RefreshControl, Modal,
} from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';
import BrandedLoading from '../../components/BrandedLoading';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

interface CustomersProps {
  sessionToken: string | null;
}

interface User {
  user_id: string;
  email: string;
  name?: string;
  subscription_tier: 'free' | 'pro' | 'pro_plus';
  is_suspended?: boolean;
  created_at?: string;
  last_login?: string;
  portfolio_count?: number;
  watchlist_count?: number;
}

type FilterTab = 'all' | 'pro' | 'free';
type ActionModal = { user: User; action: 'tier' | 'suspend' | 'delete' } | null;

function formatDate(iso?: string): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  } catch { return iso; }
}

function TierBadge({ tier }: { tier: string }) {
  const isPro = tier === 'pro' || tier === 'pro_plus';
  return (
    <View style={[sb.tierBadge, { backgroundColor: isPro ? '#F59E0B22' : '#6B728022' }]}>
      <Text style={[sb.tierText, { color: isPro ? '#F59E0B' : COLORS.textMuted }]}>
        {tier === 'pro_plus' ? 'PRO+' : tier?.toUpperCase() || 'FREE'}
      </Text>
    </View>
  );
}

export default function CustomersTab({ sessionToken }: CustomersProps) {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState<FilterTab>('all');
  const [actionModal, setActionModal] = useState<ActionModal>(null);
  const [actionLoading, setActionLoading] = useState(false);
  const [actionResult, setActionResult] = useState('');
  const [total, setTotal] = useState(0);

  const headers = {
    'Content-Type': 'application/json',
    ...(sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}),
  };

  const fetchUsers = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/api/admin/users?limit=200`, { headers });
      if (res.ok) {
        const data = await res.json();
        setUsers(data.users || []);
        setTotal(data.total || 0);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken]);

  useEffect(() => { fetchUsers(); }, [fetchUsers]);
  const onRefresh = () => { setRefreshing(true); fetchUsers(); };

  const filtered = users.filter(u => {
    const matchFilter =
      filter === 'all' ||
      (filter === 'pro' && (u.subscription_tier === 'pro' || u.subscription_tier === 'pro_plus')) ||
      (filter === 'free' && (!u.subscription_tier || u.subscription_tier === 'free'));
    const q = search.toLowerCase();
    const matchSearch = !q || u.email?.toLowerCase().includes(q) || u.name?.toLowerCase().includes(q);
    return matchFilter && matchSearch;
  });

  const proCount = users.filter(u => u.subscription_tier === 'pro' || u.subscription_tier === 'pro_plus').length;
  const freeCount = users.filter(u => !u.subscription_tier || u.subscription_tier === 'free').length;

  const executeAction = async () => {
    if (!actionModal) return;
    setActionLoading(true);
    setActionResult('');
    const { user, action } = actionModal;

    try {
      let res;
      if (action === 'tier') {
        const newTier = (user.subscription_tier === 'pro' || user.subscription_tier === 'pro_plus') ? 'free' : 'pro';
        res = await fetch(`${API_URL}/api/admin/users/${user.user_id}/tier`, {
          method: 'PATCH',
          headers,
          body: JSON.stringify({ subscription_tier: newTier }),
        });
      } else if (action === 'suspend') {
        res = await fetch(`${API_URL}/api/admin/users/${user.user_id}/suspend`, {
          method: 'POST',
          headers,
        });
      } else if (action === 'delete') {
        res = await fetch(`${API_URL}/api/admin/users/${user.user_id}`, {
          method: 'DELETE',
          headers,
        });
      }

      if (res?.ok) {
        setActionResult('✅ Done');
        setTimeout(() => {
          setActionModal(null);
          setActionResult('');
          fetchUsers();
        }, 1000);
      } else {
        const err = await res?.json();
        setActionResult(`❌ ${err?.error || 'Error'}`);
      }
    } catch (e: any) {
      setActionResult(`❌ ${e.message}`);
    } finally {
      setActionLoading(false);
    }
  };

  if (loading) return (
    <BrandedLoading message="Loading Customers..." subtitle="Fetching user data." />
  );

  return (
    <View style={s.container}>
      {/* Stats */}
      <View style={s.statsRow}>
        <View style={s.statCard}>
          <Text style={s.statNum}>{total}</Text>
          <Text style={s.statLabel}>Total Users</Text>
        </View>
        <View style={s.statCard}>
          <Text style={[s.statNum, { color: '#F59E0B' }]}>{proCount}</Text>
          <Text style={s.statLabel}>PRO</Text>
        </View>
        <View style={s.statCard}>
          <Text style={s.statNum}>{freeCount}</Text>
          <Text style={s.statLabel}>FREE</Text>
        </View>
        <View style={s.statCard}>
          <Text style={s.statNum}>{users.filter(u => u.is_suspended).length}</Text>
          <Text style={[s.statLabel, { color: '#EF4444' }]}>Suspended</Text>
        </View>
      </View>

      {/* Search */}
      <View style={s.searchWrap}>
        <Ionicons name="search" size={14} color={COLORS.textMuted} style={{ marginRight: 6 }} />
        <TextInput
          style={s.searchInput}
          placeholder="Search name, email..."
          placeholderTextColor={COLORS.textMuted}
          value={search}
          onChangeText={setSearch}
        />
        {search ? (
          <TouchableOpacity onPress={() => setSearch('')}>
            <Ionicons name="close-circle" size={14} color={COLORS.textMuted} />
          </TouchableOpacity>
        ) : null}
      </View>

      {/* Filter Tabs */}
      <View style={s.filterRow}>
        {(['all', 'pro', 'free'] as FilterTab[]).map(tab => (
          <TouchableOpacity
            key={tab}
            style={[s.filterTab, filter === tab && s.filterTabActive]}
            onPress={() => setFilter(tab)}
          >
            <Text style={[s.filterTabText, filter === tab && s.filterTabTextActive]}>
              {tab === 'all' ? `All (${total})` : tab === 'pro' ? `PRO (${proCount})` : `FREE (${freeCount})`}
            </Text>
          </TouchableOpacity>
        ))}
      </View>

      {/* User List */}
      <ScrollView
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={COLORS.primary} />}
      >
        {filtered.length === 0 ? (
          <View style={s.empty}>
            <Ionicons name="people-outline" size={32} color={COLORS.textMuted} />
            <Text style={s.emptyText}>No users found</Text>
          </View>
        ) : (
          filtered.map(user => (
            <View key={user.user_id} style={[s.userCard, user.is_suspended && s.userCardSuspended]}>
              <View style={s.userAvatar}>
                <Text style={s.userAvatarText}>{(user.name || user.email || '?')[0].toUpperCase()}</Text>
              </View>
              <View style={s.userInfo}>
                <View style={s.userTopRow}>
                  <Text style={s.userName} numberOfLines={1}>{user.name || '—'}</Text>
                  <TierBadge tier={user.subscription_tier} />
                  {user.is_suspended && (
                    <View style={s.suspendedBadge}>
                      <Text style={s.suspendedText}>SUSPENDED</Text>
                    </View>
                  )}
                </View>
                <Text style={s.userEmail} numberOfLines={1}>{user.email}</Text>
                <View style={s.userStats}>
                  <Text style={s.userStat}>
                    <Ionicons name="briefcase-outline" size={10} color={COLORS.textMuted} /> {user.portfolio_count ?? 0} portfolios
                  </Text>
                  <Text style={s.userStat}>
                    <Ionicons name="bookmark-outline" size={10} color={COLORS.textMuted} /> {user.watchlist_count ?? 0} following
                  </Text>
                  <Text style={s.userStat}>Joined {formatDate(user.created_at)}</Text>
                </View>
              </View>
              {/* Actions */}
              <View style={s.userActions}>
                <TouchableOpacity
                  style={s.actionBtn}
                  onPress={() => setActionModal({ user, action: 'tier' })}
                >
                  <Ionicons name="swap-vertical-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
                <TouchableOpacity
                  style={s.actionBtn}
                  onPress={() => setActionModal({ user, action: 'suspend' })}
                >
                  <Ionicons
                    name={user.is_suspended ? 'play-circle-outline' : 'pause-circle-outline'}
                    size={14}
                    color={user.is_suspended ? '#22C55E' : '#F59E0B'}
                  />
                </TouchableOpacity>
                <TouchableOpacity
                  style={s.actionBtn}
                  onPress={() => setActionModal({ user, action: 'delete' })}
                >
                  <Ionicons name="trash-outline" size={14} color="#EF4444" />
                </TouchableOpacity>
              </View>
            </View>
          ))
        )}
        <View style={{ height: 40 }} />
      </ScrollView>

      {/* Action Modal */}
      <Modal visible={!!actionModal} transparent animationType="fade">
        <TouchableOpacity style={s.modalOverlay} activeOpacity={1} onPress={() => !actionLoading && setActionModal(null)}>
          <TouchableOpacity style={s.modalCard} activeOpacity={1}>
            {actionModal && (
              <>
                <Text style={s.modalTitle}>
                  {actionModal.action === 'tier' && (
                    (actionModal.user.subscription_tier === 'pro' || actionModal.user.subscription_tier === 'pro_plus')
                      ? '⬇️ Downgrade to FREE'
                      : '⬆️ Upgrade to PRO'
                  )}
                  {actionModal.action === 'suspend' && (actionModal.user.is_suspended ? '▶️ Unsuspend User' : '⏸️ Suspend User')}
                  {actionModal.action === 'delete' && '🗑️ Delete User'}
                </Text>
                <Text style={s.modalUser}>{actionModal.user.email}</Text>
                <Text style={s.modalDesc}>
                  {actionModal.action === 'tier' && 'This will change the user subscription tier immediately.'}
                  {actionModal.action === 'suspend' && (actionModal.user.is_suspended
                    ? 'User will regain access to the app.'
                    : 'User will not be able to log in.')}
                  {actionModal.action === 'delete' && '⚠️ This permanently deletes the user and all their data (portfolios, watchlist, sessions). Cannot be undone.'}
                </Text>
                {actionResult ? (
                  <Text style={s.actionResult}>{actionResult}</Text>
                ) : (
                  <View style={s.modalBtns}>
                    <TouchableOpacity style={s.cancelBtn} onPress={() => setActionModal(null)}>
                      <Text style={s.cancelBtnText}>Cancel</Text>
                    </TouchableOpacity>
                    <TouchableOpacity
                      style={[s.confirmBtn, actionModal.action === 'delete' && s.confirmBtnDanger]}
                      onPress={executeAction}
                      disabled={actionLoading}
                    >
                      {actionLoading
                        ? <ActivityIndicator size="small" color="#fff" />
                        : <Text style={s.confirmBtnText}>Confirm</Text>
                      }
                    </TouchableOpacity>
                  </View>
                )}
              </>
            )}
          </TouchableOpacity>
        </TouchableOpacity>
      </Modal>
    </View>
  );
}

const sb = StyleSheet.create({
  tierBadge: { paddingHorizontal: 5, paddingVertical: 1, borderRadius: 4 },
  tierText: { fontSize: 9, fontWeight: '700', letterSpacing: 0.5 },
});

const s = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center' },

  statsRow: { flexDirection: 'row', padding: 12, gap: 8 },
  statCard: { flex: 1, backgroundColor: COLORS.card, borderRadius: 8, padding: 10, alignItems: 'center', borderWidth: 1, borderColor: COLORS.border },
  statNum: { fontSize: 18, fontWeight: '800', color: COLORS.text },
  statLabel: { fontSize: 10, color: COLORS.textMuted, marginTop: 2 },

  searchWrap: { flexDirection: 'row', alignItems: 'center', marginHorizontal: 12, marginBottom: 8, backgroundColor: COLORS.card, borderRadius: 8, paddingHorizontal: 10, paddingVertical: 8, borderWidth: 1, borderColor: COLORS.border },
  searchInput: { flex: 1, fontSize: 13, color: COLORS.text, padding: 0 },

  filterRow: { flexDirection: 'row', marginHorizontal: 12, marginBottom: 8, backgroundColor: COLORS.card, borderRadius: 8, padding: 3, borderWidth: 1, borderColor: COLORS.border },
  filterTab: { flex: 1, paddingVertical: 5, alignItems: 'center', borderRadius: 6 },
  filterTabActive: { backgroundColor: COLORS.primary },
  filterTabText: { fontSize: 11, color: COLORS.textMuted, fontWeight: '500' },
  filterTabTextActive: { color: '#fff', fontWeight: '700' },

  empty: { alignItems: 'center', padding: 40, gap: 8 },
  emptyText: { fontSize: 13, color: COLORS.textMuted },

  userCard: { flexDirection: 'row', alignItems: 'center', marginHorizontal: 12, marginBottom: 8, backgroundColor: COLORS.card, borderRadius: 10, padding: 10, borderWidth: 1, borderColor: COLORS.border, gap: 10 },
  userCardSuspended: { opacity: 0.6, borderColor: '#EF444444' },
  userAvatar: { width: 36, height: 36, borderRadius: 18, backgroundColor: COLORS.primary + '22', alignItems: 'center', justifyContent: 'center' },
  userAvatarText: { fontSize: 14, fontWeight: '700', color: COLORS.primary },
  userInfo: { flex: 1, minWidth: 0 },
  userTopRow: { flexDirection: 'row', alignItems: 'center', gap: 5, marginBottom: 2 },
  userName: { fontSize: 13, fontWeight: '600', color: COLORS.text, flex: 1 },
  userEmail: { fontSize: 11, color: COLORS.textMuted, marginBottom: 3 },
  userStats: { flexDirection: 'row', gap: 8, flexWrap: 'wrap' },
  userStat: { fontSize: 10, color: COLORS.textMuted },
  suspendedBadge: { backgroundColor: '#EF444422', paddingHorizontal: 4, paddingVertical: 1, borderRadius: 3 },
  suspendedText: { fontSize: 9, color: '#EF4444', fontWeight: '700' },

  userActions: { flexDirection: 'column', gap: 4 },
  actionBtn: { padding: 4, borderRadius: 4, backgroundColor: COLORS.border },

  modalOverlay: { flex: 1, backgroundColor: '#00000066', justifyContent: 'center', alignItems: 'center' },
  modalCard: { backgroundColor: COLORS.card, borderRadius: 14, padding: 20, width: '85%', maxWidth: 340, borderWidth: 1, borderColor: COLORS.border },
  modalTitle: { fontSize: 15, fontWeight: '700', color: COLORS.text, marginBottom: 6 },
  modalUser: { fontSize: 12, color: COLORS.textMuted, marginBottom: 10 },
  modalDesc: { fontSize: 12, color: COLORS.text, lineHeight: 18, marginBottom: 16 },
  actionResult: { fontSize: 14, textAlign: 'center', paddingVertical: 8 },
  modalBtns: { flexDirection: 'row', gap: 8 },
  cancelBtn: { flex: 1, padding: 10, borderRadius: 8, borderWidth: 1, borderColor: COLORS.border, alignItems: 'center' },
  cancelBtnText: { fontSize: 13, color: COLORS.textMuted, fontWeight: '600' },
  confirmBtn: { flex: 1, padding: 10, borderRadius: 8, backgroundColor: COLORS.primary, alignItems: 'center' },
  confirmBtnDanger: { backgroundColor: '#EF4444' },
  confirmBtnText: { fontSize: 13, color: '#fff', fontWeight: '600' },
});
