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
  orientation?: 'horizontal' | 'vertical';
}

export function NavLinks({ current, orientation = 'horizontal' }: NavLinksProps) {
  const isVertical = orientation === 'vertical';

  return (
    <div className={`hero-actions${isVertical ? ' hero-actions-vertical hero-links-column' : ''}`}>
      {ALL_LINKS.filter((link) => link.href !== current).map((link) => (
        <a
          key={link.href}
          href={link.href}
          className={`hero-action${
            link.href === '/trade-log' || link.href === '/live-diary'
              ? ' hero-action-pill hero-action-blue'
              : link.href === '/replay' ||
                  link.href === '/backtest-trades' ||
                  link.href === '/backtest-diary'
                  ? ' hero-action-pill hero-action-restart'
                  : link.href === '/'
                    ? ' hero-action-pill hero-action-home'
                  : ''
          }`}
          target={link.href === '/' ? undefined : '_blank'}
          rel={link.href === '/' ? undefined : 'noreferrer'}
        >
          {link.href === '/' ? 'Back to Live Board' : link.label}
        </a>
      ))}
    </div>
  );
}
