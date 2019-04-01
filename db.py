import json
import os

from fetcher import Profile, Coordinator, Rule

def read_static_data(coordinator, chan_dir):
    files = os.listdir(chan_dir)
    for file in files:
        path = os.path.join(chan_dir, file)
        with open(path, 'r') as f:
            channel_meta = json.load(f)
            p = Profile.from_channel_meta(coordinator, channel_meta)
            assert ('channel-%s.json' % p.ch_id) == file
            coordinator.add_profile(p)

    import get_ia
    ia_ids = get_ia.get_ia_ids()
    for profile in coordinator.get_profiles():
        for vid in profile.get_videos():
            vid.set_in_ia(vid.id in ia_ids)

def print_rules(rules):
    for i, rule in enumerate(rules):
        print('%d:' % i, rule.json())

def run_cmd(coordinator, cmd):
    if cmd == 'reset':
        coordinator.clear()
        read_static_data(coordinator, 'channels')
        coordinator.save_state()
    elif cmd == 'getrules':
        print("Global Rules")
        print_rules(coordinator.get_rules())
        for profile in coordinator.get_profiles():
            rules = profile.get_rules()
            if rules:
                print("Profile rules [%d]" % profile.ch_id)
                print_rules(rules)
    elif cmd.startswith('addrule'):
        args = cmd.split(' ')
        if args[1] == 'global':
            place_to_add = coordinator
        else:
            ch_id = args[1]
            place_to_add = coordinator.profiles[ch_id]
        rule = Rule.parse(args[2:])
        place_to_add.add_rule(rule)
        coordinator.save_state()
    elif cmd.startswith('remrule'):
        where, idx = cmd.split(' ')[1:]
        idx = int(idx)
        if where == 'global':
            place_to_rem = coordinator
        else:
            place_to_rem = coordinator.profiles[where]
        place_to_rem.remove_rule(idx)
        coordinator.save_state()
    elif cmd == 'getdone' or cmd == 'gettodo':
        for profile in coordinator.get_profiles():
            for vid in profile.get_filtered_videos(coordinator.get_rules()):
                if vid.video_data is not None and cmd == 'getdone':
                    print(vid.video_data['filename'])
                elif vid.video_data is None and cmd == 'gettodo':
                    print(vid.id, vid.title)
    else:
        assert False


if __name__ == '__main__':
    coordinator = Coordinator('state.json')
    coordinator.load_state()
    while True:
        cmd = input('db > ')
        if cmd in ['q', 'exit', 'quit']:
            break
        run_cmd(coordinator, cmd)
