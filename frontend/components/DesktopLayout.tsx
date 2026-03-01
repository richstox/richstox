import React from 'react';
import { View, StyleSheet, useWindowDimensions, Platform, ScrollView } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

interface DesktopLayoutProps {
  children: React.ReactNode;
  sidebar?: React.ReactNode;
  maxWidth?: number;
}

export const useIsDesktop = () => {
  const { width } = useWindowDimensions();
  return width >= 768;
};

export const useIsLargeDesktop = () => {
  const { width } = useWindowDimensions();
  return width >= 1200;
};

export const DesktopLayout: React.FC<DesktopLayoutProps> = ({ 
  children, 
  sidebar,
  maxWidth = 1400 
}) => {
  const { width } = useWindowDimensions();
  const isDesktop = width >= 768;
  const isLargeDesktop = width >= 1200;

  if (!isDesktop) {
    // Mobile layout - just render children
    return <>{children}</>;
  }

  return (
    <View style={styles.desktopContainer}>
      <View style={[styles.desktopContent, { maxWidth }]}>
        {sidebar && isLargeDesktop && (
          <View style={styles.sidebar}>
            {sidebar}
          </View>
        )}
        <View style={[styles.mainContent, isLargeDesktop && sidebar && styles.mainContentWithSidebar]}>
          {children}
        </View>
      </View>
    </View>
  );
};

export const ContentCard: React.FC<{
  children: React.ReactNode;
  style?: any;
}> = ({ children, style }) => {
  return (
    <View style={[styles.card, style]}>
      {children}
    </View>
  );
};

export const ResponsiveGrid: React.FC<{
  children: React.ReactNode;
  columns?: { mobile: number; tablet: number; desktop: number };
  gap?: number;
}> = ({ 
  children, 
  columns = { mobile: 1, tablet: 2, desktop: 3 },
  gap = 16 
}) => {
  const { width } = useWindowDimensions();
  
  let numColumns = columns.mobile;
  if (width >= 768) numColumns = columns.tablet;
  if (width >= 1200) numColumns = columns.desktop;

  return (
    <View style={[styles.grid, { gap }]}>
      {React.Children.map(children, (child, index) => (
        <View style={[styles.gridItem, { 
          width: `${100 / numColumns}%`,
          paddingRight: (index + 1) % numColumns !== 0 ? gap / 2 : 0,
          paddingLeft: index % numColumns !== 0 ? gap / 2 : 0,
        }]}>
          {child}
        </View>
      ))}
    </View>
  );
};

const styles = StyleSheet.create({
  desktopContainer: {
    flex: 1,
    alignItems: 'center',
    backgroundColor: '#F8FAFC',
  },
  desktopContent: {
    flex: 1,
    width: '100%',
    flexDirection: 'row',
  },
  sidebar: {
    width: 280,
    backgroundColor: '#FFFFFF',
    borderRightWidth: 1,
    borderRightColor: '#E5E7EB',
    paddingVertical: 24,
    paddingHorizontal: 16,
  },
  mainContent: {
    flex: 1,
  },
  mainContentWithSidebar: {
    paddingLeft: 0,
  },
  card: {
    backgroundColor: '#FFFFFF',
    borderRadius: 16,
    padding: 20,
    ...Platform.select({
      web: {
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      },
      default: {
        shadowColor: '#000',
        shadowOffset: { width: 0, height: 1 },
        shadowOpacity: 0.1,
        shadowRadius: 3,
        elevation: 2,
      },
    }),
  },
  grid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
  },
  gridItem: {
    marginBottom: 16,
  },
});

export default DesktopLayout;
