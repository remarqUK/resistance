import React from 'react';

const ALL_LINKS = [
  { href: '/', label: 'Live Board' },
  // Live group
  { href: '/trade-log', label: 'Live Trade Log' },
  { href: '/live-diary', label: 'Live Diary' },
  // Backtest group
  { href: '/backtest-trades', label: 'Backtest Trades' },
  { href: '/backtest-diary', label: 'Backtest Diary' },
  // Tools
  { href: '/replay', label: 'Strategy Replay' },
];

interface NavLinksProps {
  current?: string;
}

export function NavLinks({ current }: NavLinksProps) {
  return (
    <div className="hero-actions hero-actions-vertical hero-links-column">
      {ALL_LINKS.filter((link) => link.href !== current).map((link) => (
        <a
          key={link.href}
          href={link.href}
          className="hero-action"
          target={link.href === '/' ? undefined : '_blank'}
          rel={link.href === '/' ? undefined : 'noreferrer'}
        >
          {link.href === '/' ? 'Back to Live Board' : link.label}
        </a>
      ))}
    </div>
  );
}
