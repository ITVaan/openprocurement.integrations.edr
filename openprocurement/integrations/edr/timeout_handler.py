class TimeoutHandler:
    def __init__(self, min=1, max=300, step=2, mode='mult'):
        self.min = min
        self.max = max
        self.step = step
        self.mode = mode
        self.value = min

        self.updater_func = self._update_mult if self.mode == 'mult' else self._update_add

    def update(self, is_success):
        return self.updater_func(is_success)

    def _update_add(self, is_success):
        if is_success and self.value > self.min:
            self.value -= self.step
            if self.value < self.min:
                self.value = self.min

        if not is_success and self.value < self.max:
            self.value += self.step
            if self.value > self.max:
                self.value = self.max
        elif not is_success:  # already was max
            return False

        return True

    def _update_mult(self, is_success):
        if is_success and self.value > self.min:
            self.value = self.value / self.step
            if self.value < self.min:
                self.value = self.min

        if not is_success and self.value < self.max:
            self.value *= self.step
            if self.value > self.max:
                self.value = self.max
                return False
        elif not is_success:  # already was max
            return False

        return True
