import React from 'react';
import { View, Text, StyleSheet, Platform } from 'react-native';
import Svg, { Path, Circle, G } from 'react-native-svg';

interface LogoProps {
  size?: 'small' | 'medium' | 'large';
  showText?: boolean;
  variant?: 'light' | 'dark';
}

export const RichstoxLogo: React.FC<LogoProps> = ({ 
  size = 'medium', 
  showText = true,
  variant = 'dark'
}) => {
  const dimensions = {
    small: { icon: 24, fontSize: 16, gap: 6 },
    medium: { icon: 32, fontSize: 22, gap: 8 },
    large: { icon: 48, fontSize: 32, gap: 12 },
  };

  const { icon, fontSize, gap } = dimensions[size];
  const primaryColor = variant === 'dark' ? '#1A365D' : '#FFFFFF';
  const accentColor = '#009688';

  return (
    <View style={[styles.container, { gap }]}>
      {/* Logo Icon - Stylized R with upward trend */}
      <Svg width={icon} height={icon} viewBox="0 0 48 48">
        <G>
          {/* Background circle */}
          <Circle cx="24" cy="24" r="22" fill={primaryColor} />
          
          {/* Stylized R letter */}
          <Path
            d="M16 36V12h8c2.5 0 4.5 0.5 6 1.5s2.25 2.5 2.25 4.5c0 1.5-0.5 2.75-1.5 3.75s-2.25 1.75-3.75 2.25L34 36h-5l-6-11h-2v11h-5z M21 21h2.5c1 0 1.75-0.25 2.25-0.75s0.75-1.25 0.75-2.25c0-0.75-0.25-1.5-0.75-2s-1.25-0.75-2.25-0.75H21v5.75z"
            fill="#FFFFFF"
          />
          
          {/* Upward trend arrow */}
          <Path
            d="M30 14l6 0l0 6M36 14l-8 8"
            stroke={accentColor}
            strokeWidth="2.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            fill="none"
          />
        </G>
      </Svg>

      {showText && (
        <View style={styles.textContainer}>
          <Text style={[styles.logoText, { fontSize, color: primaryColor }]}>
            RICHSTOX
          </Text>
        </View>
      )}
    </View>
  );
};

// Simple text-based logo for compatibility
export const RichstoxLogoText: React.FC<LogoProps> = ({ 
  size = 'medium',
  variant = 'dark'
}) => {
  const fontSizes = { small: 18, medium: 24, large: 36 };
  const primaryColor = variant === 'dark' ? '#1A365D' : '#FFFFFF';
  const accentColor = '#009688';

  return (
    <View style={styles.textLogoContainer}>
      <Text style={[styles.textLogo, { fontSize: fontSizes[size], color: primaryColor }]}>
        RICH
      </Text>
      <Text style={[styles.textLogo, { fontSize: fontSizes[size], color: accentColor }]}>
        STOX
      </Text>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  textContainer: {
    flexDirection: 'row',
  },
  logoText: {
    fontWeight: '800',
    letterSpacing: 2,
  },
  textLogoContainer: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  textLogo: {
    fontWeight: '800',
    letterSpacing: 2,
  },
});

export default RichstoxLogo;
