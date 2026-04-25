import React, { useCallback, useEffect, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  TouchableOpacity,
  ActivityIndicator,
  Image,
  ScrollView,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter, useLocalSearchParams } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { API_URL } from '../../../utils/config';
import { useLayoutSpacing } from '../../../constants/layout';

const COLORS = {
  primary: '#4A6FA5',
  text: '#2D3436',
  textLight: '#636E72',
  textMuted: '#95A5A6',
  background: '#F8F6F3',
  card: '#FFFFFF',
  border: '#E8E4DF',
  positive: '#059669',
  negative: '#DC2626',
  tabActive: '#4A6FA5',
  tabInactive: '#95A5A6',
};

function formatMarketCap(n: number | null | undefined): string {
  if (!n || n <= 0) return '—';
  if (n >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
  if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  return `$${n.toFixed(0)}`;
}

function formatPct(n: number | null | undefined, decimals = 1): string {
  if (n === null || n === undefined) return '—';
  const sign = n >= 0 ? '+' : '';
  return `${sign}${n.toFixed(decimals)}%`;
}

function formatPE(n: number | null | undefined): string {
  if (n === null || n === undefined) return '—';
  return n.toFixed(1);
}

type SortField = 'market_cap' | 'return_1y' | 'max_drawdown_1y' | 'pe_ratio' | 'div_yield';
type SortDir = 'desc' | 'asc';
type TabKey = 'performance' | 'fundamentals';

type Company = {
  ticker: string;
  name: string;
  logo: string | null;
  market_cap: number | null;
  return_1y: number | null;
  max_drawdown_1y: number | null;
  pe_ratio: number | null;
  div_yield: number | null;
};

function IndustryLogo({ uri, ticker }: { uri: string | null; ticker: string }) {
  const [imgError, setImgError] = React.useState(false);
  if (!uri || imgError) {
    return (
      <View style={[styles.rowLogo, styles.rowLogoFallback]}>
        <Text style={styles.rowLogoInitial}>{ticker[0]}</Text>
      </View>
    );
  }
  return (
    <Image
      source={{ uri: uri.startsWith('http') ? uri : `${API_URL}${uri}` }}
      style={styles.rowLogo}
      onError={() => setImgError(true)}
    />
  );
}

function SortableHeader({
  label,
  field,
  sortField,
  sortDir,
  onPress,
  align = 'right',
}: {
  label: string;
  field: SortField;
  sortField: SortField;
  sortDir: SortDir;
  onPress: (f: SortField) => void;
  align?: 'left' | 'right';
}) {
  const active = sortField === field;
  return (
    <TouchableOpacity
      onPress={() => onPress(field)}
      style={[styles.colHeader, align === 'right' && styles.colHeaderRight]}
    >
      <Text style={[styles.colHeaderText, active && styles.colHeaderActive]}>{label}</Text>
      {active && (
        <Ionicons
          name={sortDir === 'desc' ? 'chevron-down' : 'chevron-up'}
          size={11}
          color={COLORS.tabActive}
        />
      )}
    </TouchableOpacity>
  );
}

export default function IndustryDetailScreen() {
  const router = useRouter();
  const sp = useLayoutSpacing();
  const { name } = useLocalSearchParams<{ name: string }>();

  const [companies, setCompanies] = useState<Company[]>([]);
  const [sector, setSector] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>('performance');
  const [sortField, setSortField] = useState<SortField>('market_cap');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const loadCompanies = useCallback(async () => {
    if (!name) return;
    setLoading(true);
    setError(null);
    try {
      const encodedName = encodeURIComponent(name);
      const res = await axios.get(`${API_URL}/api/v1/browse/industries/${encodedName}`);
      setCompanies(res.data.companies || []);
      setSector(res.data.sector || null);
    } catch {
      setError('Could not load companies. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    loadCompanies();
  }, [loadCompanies]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
    } else {
      setSortField(field);
      setSortDir('desc');
    }
  };

  const sortedCompanies = [...companies].sort((a, b) => {
    const va = a[sortField] ?? (sortDir === 'desc' ? -Infinity : Infinity);
    const vb = b[sortField] ?? (sortDir === 'desc' ? -Infinity : Infinity);
    return sortDir === 'desc' ? (vb as number) - (va as number) : (va as number) - (vb as number);
  });

  const handleTickerPress = (ticker: string) => {
    router.push(`/stock/${ticker}`);
  };

  const handleTabChange = (newTab: TabKey) => {
    setTab(newTab);
    setSortField('market_cap');
    setSortDir('desc');
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* Header */}
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <View style={styles.headerCenter}>
          <Text style={styles.headerTitle} numberOfLines={1}>{name || 'Industry'}</Text>
        </View>
        <View style={styles.headerPlaceholder} />
      </View>

      {loading ? (
        <View style={styles.center}>
          <ActivityIndicator size="large" color={COLORS.primary} />
        </View>
      ) : error ? (
        <View style={styles.center}>
          <Text style={styles.errorText}>{error}</Text>
          <TouchableOpacity style={styles.retryButton} onPress={loadCompanies}>
            <Text style={styles.retryText}>Retry</Text>
          </TouchableOpacity>
        </View>
      ) : (
        <>
          {/* Tab bar */}
          <View style={styles.tabBar}>
            <TouchableOpacity
              style={[styles.tabItem, tab === 'performance' && styles.tabItemActive]}
              onPress={() => handleTabChange('performance')}
            >
              <Text style={[styles.tabLabel, tab === 'performance' && styles.tabLabelActive]}>
                Performance
              </Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[styles.tabItem, tab === 'fundamentals' && styles.tabItemActive]}
              onPress={() => handleTabChange('fundamentals')}
            >
              <Text style={[styles.tabLabel, tab === 'fundamentals' && styles.tabLabelActive]}>
                Fundamentals
              </Text>
            </TouchableOpacity>
          </View>

          {/* Column headers */}
          <View style={[styles.tableHeader, { paddingHorizontal: sp.pageGutter }]}>
            <View style={styles.colCompany}>
              <Text style={styles.colHeaderText}>Company</Text>
            </View>
            <SortableHeader
              label="Mkt Cap"
              field="market_cap"
              sortField={sortField}
              sortDir={sortDir}
              onPress={handleSort}
            />
            {tab === 'performance' ? (
              <>
                <SortableHeader
                  label="1Y Return"
                  field="return_1y"
                  sortField={sortField}
                  sortDir={sortDir}
                  onPress={handleSort}
                />
                <SortableHeader
                  label="Max DD"
                  field="max_drawdown_1y"
                  sortField={sortField}
                  sortDir={sortDir}
                  onPress={handleSort}
                />
              </>
            ) : (
              <>
                <SortableHeader
                  label="P/E"
                  field="pe_ratio"
                  sortField={sortField}
                  sortDir={sortDir}
                  onPress={handleSort}
                />
                <SortableHeader
                  label="Div Yield"
                  field="div_yield"
                  sortField={sortField}
                  sortDir={sortDir}
                  onPress={handleSort}
                />
              </>
            )}
          </View>

          <FlatList
            data={sortedCompanies}
            keyExtractor={(item) => item.ticker}
            contentContainerStyle={[styles.list, { paddingHorizontal: sp.pageGutter }]}
            showsVerticalScrollIndicator={false}
            ListHeaderComponent={
              <Text style={styles.countLabel}>
                {companies.length} {companies.length === 1 ? 'company' : 'companies'} in {name}
              </Text>
            }
            renderItem={({ item }) => (
              <TouchableOpacity
                style={styles.row}
                onPress={() => handleTickerPress(item.ticker)}
                activeOpacity={0.75}
              >
                {/* Logo + name */}
                <View style={styles.colCompany}>
                  <IndustryLogo uri={item.logo} ticker={item.ticker} />
                  <View style={styles.rowNameWrap}>
                    <Text style={styles.rowTicker}>{item.ticker}</Text>
                    <Text style={styles.rowName} numberOfLines={1}>{item.name}</Text>
                  </View>
                </View>

                {/* Market cap */}
                <Text style={[styles.colValue, styles.colRight]}>
                  {formatMarketCap(item.market_cap)}
                </Text>

                {tab === 'performance' ? (
                  <>
                    <Text style={[
                      styles.colValue, styles.colRight,
                      item.return_1y !== null && item.return_1y >= 0 ? styles.positive : styles.negative,
                    ]}>
                      {formatPct(item.return_1y)}
                    </Text>
                    <Text style={[
                      styles.colValue, styles.colRight,
                      item.max_drawdown_1y !== null && item.max_drawdown_1y < 0 ? styles.negative : styles.neutral,
                    ]}>
                      {item.max_drawdown_1y !== null ? formatPct(item.max_drawdown_1y) : '—'}
                    </Text>
                  </>
                ) : (
                  <>
                    <Text style={[styles.colValue, styles.colRight]}>{formatPE(item.pe_ratio)}</Text>
                    <Text style={[styles.colValue, styles.colRight]}>
                      {item.div_yield !== null ? `${item.div_yield.toFixed(2)}%` : '—'}
                    </Text>
                  </>
                )}
              </TouchableOpacity>
            )}
            ItemSeparatorComponent={() => <View style={styles.separator} />}
          />
        </>
      )}
    </SafeAreaView>
  );
}

const COL_W = 72;

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 8,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  backButton: { padding: 16, marginLeft: -8 },
  headerCenter: { flex: 1, alignItems: 'center' },
  headerTitle: { fontSize: 18, fontWeight: '700', color: COLORS.text },
  headerPlaceholder: { width: 48 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: 24 },
  errorText: { fontSize: 15, color: COLORS.textLight, textAlign: 'center', marginBottom: 16 },
  retryButton: { paddingHorizontal: 20, paddingVertical: 10, borderRadius: 8, backgroundColor: COLORS.primary },
  retryText: { color: '#fff', fontWeight: '700' },
  tabBar: {
    flexDirection: 'row',
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  tabItem: {
    flex: 1,
    alignItems: 'center',
    paddingVertical: 12,
    borderBottomWidth: 2,
    borderBottomColor: 'transparent',
  },
  tabItemActive: { borderBottomColor: COLORS.tabActive },
  tabLabel: { fontSize: 14, fontWeight: '600', color: COLORS.tabInactive },
  tabLabelActive: { color: COLORS.tabActive },
  tableHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 8,
    backgroundColor: COLORS.background,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  colHeader: { width: COL_W, flexDirection: 'row', alignItems: 'center', gap: 2 },
  colHeaderRight: { justifyContent: 'flex-end' },
  colHeaderText: { fontSize: 11, fontWeight: '600', color: COLORS.textMuted, textTransform: 'uppercase' },
  colHeaderActive: { color: COLORS.tabActive },
  colCompany: { flex: 1, flexDirection: 'row', alignItems: 'center', gap: 8, minWidth: 0 },
  countLabel: { fontSize: 12, color: COLORS.textMuted, paddingVertical: 8, fontWeight: '600' },
  list: { paddingBottom: 32 },
  row: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
    backgroundColor: COLORS.card,
    borderRadius: 10,
    paddingHorizontal: 10,
    marginBottom: 4,
  },
  rowLogo: { width: 34, height: 34, borderRadius: 8, backgroundColor: '#F3F4F6', flexShrink: 0 },
  rowLogoFallback: { alignItems: 'center', justifyContent: 'center', backgroundColor: '#EEF2FF' },
  rowLogoInitial: { fontSize: 13, fontWeight: '700', color: COLORS.primary },
  rowNameWrap: { flex: 1, minWidth: 0 },
  rowTicker: { fontSize: 13, fontWeight: '700', color: COLORS.text },
  rowName: { fontSize: 11, color: COLORS.textLight },
  colValue: { width: COL_W, fontSize: 12, fontWeight: '600', color: COLORS.text },
  colRight: { textAlign: 'right' },
  positive: { color: COLORS.positive },
  negative: { color: COLORS.negative },
  neutral: { color: COLORS.textMuted },
  separator: { height: 2 },
});
