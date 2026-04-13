import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  ActivityIndicator,
  TouchableOpacity,
  Image,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useLocalSearchParams, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { API_URL } from '../../utils/config';

const COLORS = {
  primary: '#1E3A5F',
  accent: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
};

interface UserProfile {
  user_id: string;
  name: string;
  picture?: string;
  followed_companies_count: number;
  posts_count: number;
  rrr: number | null;
  total_return_365d: number | null;
  max_drawdown_365d: number | null;
  track_record_days: number | null;
  created_at: string;
}

interface TalkPost {
  post_id: string;
  text: string;
  symbol?: string;
  created_at: string;
}

export default function UserProfileScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const router = useRouter();
  const [loading, setLoading] = useState(true);
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [posts, setPosts] = useState<TalkPost[]>([]);
  const [hasMorePosts, setHasMorePosts] = useState(false);
  const [postsOffset, setPostsOffset] = useState(0);
  const [loadingPosts, setLoadingPosts] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchProfile();
    fetchPosts(0);
  }, [id]);

  const fetchProfile = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_URL}/api/v1/users/${id}`);
      setProfile(response.data);
    } catch (err: any) {
      console.error('Error fetching profile:', err);
      setError(err?.response?.data?.detail || 'User not found');
    } finally {
      setLoading(false);
    }
  };

  const fetchPosts = async (offset: number) => {
    try {
      setLoadingPosts(true);
      const response = await axios.get(`${API_URL}/api/v1/users/${id}/posts?limit=20&offset=${offset}`);
      
      if (offset === 0) {
        setPosts(response.data.posts || []);
      } else {
        setPosts([...posts, ...(response.data.posts || [])]);
      }
      
      setHasMorePosts(response.data.has_more || false);
      setPostsOffset(offset);
    } catch (err) {
      console.error('Error fetching posts:', err);
    } finally {
      setLoadingPosts(false);
    }
  };

  const loadMorePosts = () => {
    if (hasMorePosts && !loadingPosts) {
      fetchPosts(postsOffset + 20);
    }
  };

  const formatTimeAgo = (dateStr: string) => {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffDays < 1) return 'today';
    if (diffDays < 30) return `${diffDays}d ago`;
    if (diffDays < 365) return `${Math.floor(diffDays / 30)}mo ago`;
    return `${Math.floor(diffDays / 365)}y ago`;
  };

  const navigateToStock = (symbol: string) => {
    const ticker = symbol.replace('.US', '');
    router.push(`/stock/${ticker}`);
  };

  if (loading) {
    return (
      <SafeAreaView style={styles.container}>
        <View style={styles.loadingContainer}>
          <ActivityIndicator size="large" color={COLORS.primary} />
        </View>
      </SafeAreaView>
    );
  }

  if (error || !profile) {
    return (
      <SafeAreaView style={styles.container}>
        <View style={styles.header}>
          <TouchableOpacity onPress={() => router.back()} style={styles.backButton}>
            <Ionicons name="arrow-back" size={24} color={COLORS.text} />
          </TouchableOpacity>
        </View>
        <View style={styles.errorContainer}>
          <Ionicons name="person-circle-outline" size={64} color={COLORS.textMuted} />
          <Text style={styles.errorText}>{error || 'User not found'}</Text>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* Header */}
      <View style={styles.header}>
        <TouchableOpacity onPress={() => router.back()} style={styles.backButton}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <Text style={styles.headerTitle}>Profile</Text>
        <View style={{ width: 40 }} />
      </View>

      <ScrollView style={styles.content} showsVerticalScrollIndicator={false}>
        {/* Profile card */}
        <View style={styles.profileCard}>
          {profile.picture ? (
            <Image source={{ uri: profile.picture }} style={styles.profileImage} />
          ) : (
            <View style={styles.profileImagePlaceholder}>
              <Ionicons name="person" size={40} color={COLORS.textMuted} />
            </View>
          )}
          
          <Text style={styles.profileName}>{profile.name}</Text>
          
          <Text style={styles.memberSince}>
            Member since {formatTimeAgo(profile.created_at)}
          </Text>
        </View>

        {/* Stats grid */}
        <View style={styles.statsGrid}>
          <View style={styles.statItem}>
            <Text style={styles.statValue}>{profile.posts_count}</Text>
            <Text style={styles.statLabel}>Posts</Text>
          </View>
          <View style={styles.statItem}>
            <Text style={styles.statValue}>{profile.followed_companies_count}</Text>
            <Text style={styles.statLabel}>Following</Text>
          </View>
          {profile.rrr !== null && (
            <View style={styles.statItem}>
              <Text style={[styles.statValue, { color: COLORS.accent }]}>
                {profile.rrr.toFixed(2)}
              </Text>
              <Text style={styles.statLabel}>RRR</Text>
            </View>
          )}
        </View>

        {/* Performance stats */}
        {(profile.total_return_365d !== null || profile.max_drawdown_365d !== null) && (
          <View style={styles.performanceCard}>
            <Text style={styles.sectionTitle}>Performance (365d)</Text>
            <View style={styles.performanceStats}>
              {profile.total_return_365d !== null && (
                <View style={styles.perfItem}>
                  <Text style={[
                    styles.perfValue,
                    { color: profile.total_return_365d >= 0 ? COLORS.accent : COLORS.danger }
                  ]}>
                    {profile.total_return_365d >= 0 ? '+' : ''}{profile.total_return_365d.toFixed(1)}%
                  </Text>
                  <Text style={styles.perfLabel}>Return</Text>
                </View>
              )}
              {profile.max_drawdown_365d !== null && (
                <View style={styles.perfItem}>
                  <Text style={[styles.perfValue, { color: COLORS.danger }]}>
                    -{Math.abs(profile.max_drawdown_365d).toFixed(1)}%
                  </Text>
                  <Text style={styles.perfLabel}>Max DD</Text>
                </View>
              )}
              {profile.track_record_days !== null && (
                <View style={styles.perfItem}>
                  <Text style={styles.perfValue}>{profile.track_record_days}</Text>
                  <Text style={styles.perfLabel}>Days</Text>
                </View>
              )}
            </View>
          </View>
        )}

        {/* Posts section */}
        <View style={styles.postsSection}>
          <Text style={styles.sectionTitle}>Posts</Text>
          
          {posts.length === 0 ? (
            <View style={styles.emptyPosts}>
              <Text style={styles.emptyText}>No posts yet</Text>
            </View>
          ) : (
            <>
              {posts.map(post => (
                <View key={post.post_id} style={styles.postCard}>
                  <Text style={styles.postText}>{post.text}</Text>
                  <View style={styles.postFooter}>
                    {post.symbol && (
                      <TouchableOpacity
                        style={styles.symbolTag}
                        onPress={() => navigateToStock(post.symbol!)}
                      >
                        <Text style={styles.symbolTagText}>${post.symbol.replace('.US', '')}</Text>
                      </TouchableOpacity>
                    )}
                    <Text style={styles.postTime}>{formatTimeAgo(post.created_at)}</Text>
                  </View>
                </View>
              ))}
              
              {hasMorePosts && (
                <TouchableOpacity
                  style={styles.loadMoreButton}
                  onPress={loadMorePosts}
                  disabled={loadingPosts}
                >
                  {loadingPosts ? (
                    <ActivityIndicator size="small" color={COLORS.primary} />
                  ) : (
                    <Text style={styles.loadMoreText}>Load more</Text>
                  )}
                </TouchableOpacity>
              )}
            </>
          )}
        </View>

        <View style={{ height: 40 }} />
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  loadingContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  errorContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  errorText: {
    fontSize: 16,
    color: COLORS.textMuted,
    marginTop: 16,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  backButton: {
    width: 40,
    height: 40,
    justifyContent: 'center',
    alignItems: 'center',
  },
  headerTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
  },
  content: {
    flex: 1,
    padding: 16,
  },
  profileCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 24,
    alignItems: 'center',
    marginBottom: 16,
  },
  profileImage: {
    width: 80,
    height: 80,
    borderRadius: 40,
    marginBottom: 12,
  },
  profileImagePlaceholder: {
    width: 80,
    height: 80,
    borderRadius: 40,
    backgroundColor: COLORS.background,
    justifyContent: 'center',
    alignItems: 'center',
    marginBottom: 12,
  },
  profileName: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 4,
  },
  memberSince: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  statsGrid: {
    flexDirection: 'row',
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 16,
    marginBottom: 16,
  },
  statItem: {
    flex: 1,
    alignItems: 'center',
  },
  statValue: {
    fontSize: 24,
    fontWeight: '700',
    color: COLORS.text,
  },
  statLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 4,
  },
  performanceCard: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 16,
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 12,
  },
  performanceStats: {
    flexDirection: 'row',
  },
  perfItem: {
    flex: 1,
    alignItems: 'center',
  },
  perfValue: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
  },
  perfLabel: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  postsSection: {
    marginBottom: 16,
  },
  emptyPosts: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 24,
    alignItems: 'center',
  },
  emptyText: {
    fontSize: 14,
    color: COLORS.textMuted,
  },
  postCard: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 16,
    marginBottom: 8,
  },
  postText: {
    fontSize: 15,
    lineHeight: 22,
    color: COLORS.text,
    marginBottom: 8,
  },
  postFooter: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  symbolTag: {
    backgroundColor: COLORS.primary + '15',
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 4,
  },
  symbolTagText: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.primary,
  },
  postTime: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  loadMoreButton: {
    backgroundColor: COLORS.card,
    borderRadius: 8,
    paddingVertical: 12,
    alignItems: 'center',
    marginTop: 8,
  },
  loadMoreText: {
    fontSize: 14,
    fontWeight: '500',
    color: COLORS.primary,
  },
});
