import { useEffect, useState } from 'react';

interface HealthGaugeProps {
  value: number;
  size?: number;
  label?: string;
  showLabel?: boolean;
  strokeWidth?: number;
}

export function HealthGauge({ value, size = 120, label, showLabel = true, strokeWidth = 8 }: HealthGaugeProps) {
  const [animatedValue, setAnimatedValue] = useState(0);
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (animatedValue / 100) * circumference;

  useEffect(() => {
    const timer = setTimeout(() => setAnimatedValue(value), 100);
    return () => clearTimeout(timer);
  }, [value]);

  const getColor = (v: number) => {
    if (v >= 80) return '#05DF72';
    if (v >= 60) return '#FF8000';
    if (v >= 40) return '#F59E0B';
    return '#FB2C36';
  };

  const getGlow = (v: number) => {
    if (v >= 80) return '0 0 12px rgba(5, 223, 114, 0.4)';
    if (v >= 60) return '0 0 12px rgba(255, 128, 0, 0.4)';
    if (v >= 40) return '0 0 12px rgba(245, 158, 11, 0.4)';
    return '0 0 12px rgba(251, 44, 54, 0.4)';
  };

  return (
    <div className="flex flex-col items-center gap-1">
      <div className="relative" style={{ width: size, height: size }}>
        <svg
          width={size}
          height={size}
          className="transform -rotate-90"
          style={{ filter: `drop-shadow(${getGlow(value)})` }}
        >
          <circle cx={size / 2} cy={size / 2} r={radius} fill="none" stroke="#222838" strokeWidth={strokeWidth} />
          <circle
            cx={size / 2} cy={size / 2} r={radius} fill="none"
            stroke={getColor(value)} strokeWidth={strokeWidth} strokeLinecap="round"
            strokeDasharray={circumference} strokeDashoffset={offset}
            style={{ transition: 'stroke-dashoffset 1s ease-out, stroke 0.3s ease' }}
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-foreground font-mono" style={{ fontSize: size * 0.22 }}>
            {Math.round(animatedValue)}
          </span>
        </div>
      </div>
      {showLabel && label && (
        <span className="text-muted-foreground text-sm text-center leading-tight">{label}</span>
      )}
    </div>
  );
}
